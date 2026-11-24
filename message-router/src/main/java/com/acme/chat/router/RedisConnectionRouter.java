package com.acme.chat.router;

import com.acme.chat.common.redis.SessionRegistry;
import com.acme.chat.domain.DeliverEnvelope;
import com.acme.chat.domain.WsFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Cross-node dispatch: user -> devices (Redis registry) -> gateway channel (Redis Pub/Sub).
 * Routes pointing at a gateway whose liveness key expired are treated as offline.
 */
@Component
public class RedisConnectionRouter {

    public record RouteResult(Set<Long> delivered, Set<Long> offline) { }

    private final SessionRegistry registry;
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Cache<String, Boolean> gatewayAlive = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(2)).maximumSize(10_000).build();

    public RedisConnectionRouter(SessionRegistry registry, ReactiveStringRedisTemplate redis, ObjectMapper mapper) {
        this.registry = registry;
        this.redis = redis;
        this.mapper = mapper;
    }

    /** Delivers frame to every live device of each recipient, except (excludeUser, excludeDevice). */
    public Mono<RouteResult> route(WsFrame frame, Collection<Long> recipients, long excludeUser,
                                   String excludeDevice) {
        return Flux.fromIterable(recipients)
                .flatMap(uid -> deliverToUser(uid, frame, excludeUser, excludeDevice)
                        .map(online -> Map.entry(uid, online)), 128)
                .collectList()
                .map(entries -> {
                    Set<Long> delivered = new HashSet<>();
                    Set<Long> offline = new HashSet<>();
                    for (var e : entries) (e.getValue() ? delivered : offline).add(e.getKey());
                    return new RouteResult(delivered, offline);
                });
    }

    /** Fan-out-on-read: one publish; only gateways with the channel open are subscribed. */
    public Mono<Long> publishToChannel(long conversationId, WsFrame frame) {
        return Mono.fromCallable(() -> mapper.writeValueAsString(frame))
                .flatMap(json -> redis.convertAndSend("conv:" + conversationId, json));
    }

    private Mono<Boolean> deliverToUser(long userId, WsFrame frame, long excludeUser, String excludeDevice) {
        return registry.routes(userId)
                .filter(r -> !(userId == excludeUser && r.deviceId().equals(excludeDevice)))
                .filterWhen(r -> isAlive(r.gatewayId()))
                .flatMap(r -> publish(r, userId, frame))
                .count()
                .map(n -> n > 0);
    }

    private Mono<Long> publish(SessionRegistry.Route route, long userId, WsFrame frame) {
        return Mono.fromCallable(() -> mapper.writeValueAsString(
                        new DeliverEnvelope(userId, route.deviceId(), route.connId(), frame)))
                .flatMap(json -> redis.convertAndSend("gw:" + route.gatewayId(), json));
    }

    private Mono<Boolean> isAlive(String gatewayId) {
        Boolean cached = gatewayAlive.getIfPresent(gatewayId);
        if (cached != null) return Mono.just(cached);
        return registry.gatewayAlive(gatewayId).doOnNext(b -> gatewayAlive.put(gatewayId, b));
    }
}
