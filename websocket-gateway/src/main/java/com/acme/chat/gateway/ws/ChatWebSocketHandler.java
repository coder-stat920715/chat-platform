package com.acme.chat.gateway.ws;

import com.acme.chat.common.redis.SessionRegistry;
import com.acme.chat.domain.WsFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.Principal;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

@Component
public class ChatWebSocketHandler implements WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketHandler.class);

    private static final Duration PING_INTERVAL = Duration.ofSeconds(25);
    private static final long IDLE_TIMEOUT_MS = 70_000;
    private static final int OUTBOUND_BUFFER = 256;     // per-socket bound => bounded memory
    private static final int MAX_FRAME_CHARS = 64 * 1024;

    private final ConnectionRegistry registry;
    private final SessionRegistry sessionRegistry;
    private final FrameDispatcher dispatcher;
    private final ChannelSubscriptions channels;
    private final ObjectMapper mapper;

    public ChatWebSocketHandler(ConnectionRegistry registry, SessionRegistry sessionRegistry,
                                FrameDispatcher dispatcher, ChannelSubscriptions channels, ObjectMapper mapper) {
        this.registry = registry;
        this.sessionRegistry = sessionRegistry;
        this.dispatcher = dispatcher;
        this.channels = channels;
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.getHandshakeInfo().getPrincipal()
                .map(Optional::of)
                .defaultIfEmpty(Optional.<Principal>empty())
                .flatMap(p -> p.isPresent()
                        ? run(session, Long.parseLong(p.get().getName()))
                        : session.close(CloseStatus.POLICY_VIOLATION.withReason("unauthenticated")));
    }

    private Mono<Void> run(WebSocketSession session, long userId) {
        String deviceId = UriComponentsBuilder.fromUri(session.getHandshakeInfo().getUri())
                .build().getQueryParams().getFirst("deviceId");
        if (deviceId == null || deviceId.isBlank() || deviceId.length() > 64) {
            return session.close(CloseStatus.BAD_DATA.withReason("deviceId required"));
        }

        Sinks.Many<WebSocketMessage> sink = Sinks.many().unicast()
                .onBackpressureBuffer(Queues.<WebSocketMessage>get(OUTBOUND_BUFFER).get());
        Connection conn = new Connection(userId, deviceId, session, sink);
        AtomicBoolean cleaned = new AtomicBoolean(false);

        // Outbound: application frames + protocol pings, written by a single subscriber.
        Flux<WebSocketMessage> pings = Flux.interval(PING_INTERVAL).concatMap(tick -> {
            long idle = System.currentTimeMillis() - conn.lastSeenMs.get();
            if (idle > IDLE_TIMEOUT_MS) {
                return session.close(CloseStatus.GOING_AWAY.withReason("idle")).then(Mono.<WebSocketMessage>empty());
            }
            return Mono.just(session.pingMessage(f -> f.wrap(new byte[0])));
        });
        Mono<Void> outbound = session.send(sink.asFlux().mergeWith(pings));

        // Inbound: any frame (incl. PONG) counts as liveness; text is extracted synchronously because
        // the underlying buffer is released after the signal returns.
        Mono<Void> inbound = session.receive()
                .doOnNext(m -> conn.lastSeenMs.set(System.currentTimeMillis()))
                .filter(m -> m.getType() == WebSocketMessage.Type.TEXT
                        || m.getType() == WebSocketMessage.Type.BINARY)
                .map(WebSocketMessage::getPayloadAsText)
                .concatMap(text -> handleFrame(conn, text), 1)   // preserves per-connection order
                .then();

        return Mono.defer(() -> {
                    Connection previous = registry.put(conn);
                    if (previous != null) {
                        previous.session.close(CloseStatus.NORMAL.withReason("replaced")).subscribe();
                    }
                    return sessionRegistry.register(userId, deviceId, session.getId());
                })
                .then(Mono.firstWithSignal(inbound, outbound))
                .doFinally(signal -> cleanup(conn, cleaned));
    }

    private Mono<Void> handleFrame(Connection conn, String text) {
        if (text.length() > MAX_FRAME_CHARS) {
            return conn.session.close(CloseStatus.TOO_BIG_TO_PROCESS);
        }
        return Mono.fromCallable(() -> mapper.readValue(text, WsFrame.class))
                .flatMap(frame -> dispatcher.dispatch(conn, frame))
                .onErrorResume(e -> {
                    log.debug("bad frame from {}: {}", conn.key(), e.toString());
                    return Mono.empty();       // one bad frame must not kill the pipeline
                });
    }

    /** Idempotent: runs on normal close, error, cancel and idle timeout alike. */
    private void cleanup(Connection conn, AtomicBoolean cleaned) {
        if (!cleaned.compareAndSet(false, true)) return;
        registry.remove(conn);
        channels.unsubscribeAll(conn);
        conn.outbound.tryEmitComplete();
        sessionRegistry.unregister(conn.userId, conn.deviceId, conn.connId())
                .onErrorResume(e -> Mono.empty())
                .subscribe();
    }
}
