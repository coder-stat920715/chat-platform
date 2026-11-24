package com.acme.chat.gateway.ws;

import com.acme.chat.common.redis.SessionRegistry;
import com.acme.chat.domain.DeliverEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * - Subscribes to this gateway's Redis channel (gw:{id}) and writes envelopes to local sockets.
 * - Publishes the liveness key gw:{id}:alive (TTL 15 s, every 5 s) used by routers to ignore dead gateways.
 * - Batch-refreshes session TTLs for live connections (jittered ~60 s) instead of per heartbeat.
 */
@Component
public class GatewayPubSubBridge implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GatewayPubSubBridge.class);

    private final ReactiveRedisMessageListenerContainer listener;
    private final ReactiveStringRedisTemplate redis;
    private final ConnectionRegistry connections;
    private final SessionRegistry sessionRegistry;
    private final ObjectMapper mapper;

    private Disposable subscription;
    private Disposable heartbeat;
    private Disposable touch;
    private volatile boolean running;

    public GatewayPubSubBridge(ReactiveRedisMessageListenerContainer listener, ReactiveStringRedisTemplate redis,
                               ConnectionRegistry connections, SessionRegistry sessionRegistry,
                               ObjectMapper mapper) {
        this.listener = listener;
        this.redis = redis;
        this.connections = connections;
        this.sessionRegistry = sessionRegistry;
        this.mapper = mapper;
    }

    @Override
    public void start() {
        String gw = sessionRegistry.gatewayId();

        heartbeat = Flux.interval(Duration.ZERO, Duration.ofSeconds(5))
                .concatMap(t -> redis.opsForValue().set("gw:" + gw + ":alive", "1", Duration.ofSeconds(15))
                        .onErrorResume(e -> Mono.empty()))
                .subscribe();

        subscription = listener.receive(ChannelTopic.of("gw:" + gw))
                .map(ReactiveSubscription.Message::getMessage)
                .flatMap(json -> Mono.fromCallable(() -> mapper.readValue(json, DeliverEnvelope.class))
                        .onErrorResume(e -> Mono.empty()))
                .doOnNext(env -> connections.deliver(env.userId(), env.deviceId(), env.frame()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(5)))
                .subscribe(x -> { }, e -> log.error("gateway subscription died", e));

        touch = Flux.interval(Duration.ofSeconds(45 + ThreadLocalRandom.current().nextInt(30)))
                .concatMap(t -> sessionRegistry.touch(Flux.fromStream(connections.userIds()))
                        .onErrorResume(e -> Mono.empty()))
                .subscribe();

        running = true;
        log.info("gateway {} online", gw);
    }

    @Override
    public void stop() {
        running = false;
        for (Disposable d : new Disposable[]{subscription, heartbeat, touch}) {
            if (d != null) d.dispose();
        }
    }

    @Override public boolean isRunning() { return running; }
}
