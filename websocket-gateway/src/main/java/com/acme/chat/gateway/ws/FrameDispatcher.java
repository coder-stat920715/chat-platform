package com.acme.chat.gateway.ws;

import com.acme.chat.common.kafka.KafkaFactory;
import com.acme.chat.common.kafka.Topics;
import com.acme.chat.common.redis.ConversationDirectory;
import com.acme.chat.domain.ChatEvent;
import com.acme.chat.domain.DeliveryReceipt;
import com.acme.chat.domain.MessageStatus;
import com.acme.chat.domain.SendCommand;
import com.acme.chat.domain.TypingIndicator;
import com.acme.chat.domain.WsFrame;
import com.acme.chat.gateway.service.ForbiddenException;
import com.acme.chat.gateway.service.MessageIngressService;
import com.acme.chat.gateway.service.RateLimiter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

/** Routes decoded client frames to the right service. Never throws; errors become ERROR frames. */
@Component
public class FrameDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FrameDispatcher.class);

    private final MessageIngressService ingress;
    private final RateLimiter limiter;
    private final ConversationDirectory directory;
    private final KafkaSender<Long, ChatEvent> eventSender;
    private final ConnectionRegistry connections;
    private final ChannelSubscriptions channels;
    private final ObjectMapper mapper;

    public FrameDispatcher(MessageIngressService ingress, RateLimiter limiter, ConversationDirectory directory,
                           KafkaSender<Long, ChatEvent> eventSender, ConnectionRegistry connections,
                           ChannelSubscriptions channels, ObjectMapper mapper) {
        this.ingress = ingress;
        this.limiter = limiter;
        this.directory = directory;
        this.eventSender = eventSender;
        this.connections = connections;
        this.channels = channels;
        this.mapper = mapper;
    }

    public Mono<Void> dispatch(Connection c, WsFrame f) {
        Mono<Void> action = switch (f.type()) {
            case WsFrame.SEND -> send(c, f);
            case WsFrame.RECEIPT -> receipt(c, f);
            case WsFrame.TYPING -> typing(c, f);
            case WsFrame.SUBSCRIBE -> subscribe(c, f, true);
            case WsFrame.UNSUBSCRIBE -> subscribe(c, f, false);
            case WsFrame.PING -> reply(c, f, WsFrame.PONG, Map.of());
            default -> reply(c, f, WsFrame.ERROR, Map.of("error", "unknown_type"));
        };
        return action.onErrorResume(e -> {
            log.debug("frame {} from {} failed: {}", f.type(), c.key(), e.toString());
            return reply(c, f, WsFrame.ERROR, Map.of("error", errorCode(e)));
        });
    }

    private Mono<Void> send(Connection c, WsFrame f) {
        return limiter.tryAcquire(c.userId).flatMap(ok -> {
            if (!ok) return reply(c, f, WsFrame.ERROR, Map.of("error", "rate_limited"));
            SendCommand cmd = mapper.convertValue(f.payload(), SendCommand.class);
            return ingress.ingest(c.userId, c.deviceId, cmd)
                    .flatMap(ack -> reply(c, f, WsFrame.ACK, ack));
        });
    }

    private Mono<Void> receipt(Connection c, WsFrame f) {
        long convId = requireConversation(f.payload());
        long messageId = f.payload().path("messageId").asLong();
        MessageStatus status = MessageStatus.valueOf(f.payload().path("status").asText("DELIVERED"));
        return requireMember(c, convId).then(Mono.defer(() -> {
            // Built server-side so a client cannot spoof another user's receipt.
            var r = new DeliveryReceipt(convId, messageId, c.userId, c.deviceId, status, true, Instant.now());
            return KafkaFactory.send(eventSender, Topics.EVENTS, convId,
                    new ChatEvent(WsFrame.RECEIPT, convId, c.userId, mapper.valueToTree(r)));
        }));
    }

    private Mono<Void> typing(Connection c, WsFrame f) {
        long convId = requireConversation(f.payload());
        boolean typing = f.payload().path("typing").asBoolean(true);
        return requireMember(c, convId).then(Mono.defer(() -> {
            var t = new TypingIndicator(convId, c.userId, typing, Instant.now());
            return KafkaFactory.send(eventSender, Topics.EVENTS, convId,
                    new ChatEvent(WsFrame.TYPING, convId, c.userId, mapper.valueToTree(t)));
        }));
    }

    private Mono<Void> subscribe(Connection c, WsFrame f, boolean on) {
        long convId = requireConversation(f.payload());
        return requireMember(c, convId).then(Mono.fromRunnable(() -> {
            if (on) channels.subscribe(convId, c); else channels.unsubscribe(convId, c);
        }));
    }

    private Mono<Void> requireMember(Connection c, long convId) {
        return directory.isMember(convId, c.userId)
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new ForbiddenException("not a member")))
                .then();
    }

    private static long requireConversation(JsonNode payload) {
        if (payload == null || !payload.hasNonNull("conversationId")) {
            throw new IllegalArgumentException("conversationId required");
        }
        return payload.get("conversationId").asLong();
    }

    private Mono<Void> reply(Connection c, WsFrame in, String type, Object body) {
        return Mono.fromRunnable(() ->
                connections.send(c, new WsFrame(type, in.correlationId(), mapper.valueToTree(body))));
    }

    private static String errorCode(Throwable e) {
        if (e instanceof ForbiddenException) return "forbidden";
        if (e instanceof IllegalArgumentException) return "invalid";
        if (e instanceof TimeoutException) return "unavailable";
        return "internal";
    }
}
