package com.acme.chat.common.redis;

import java.time.Duration;
import java.util.List;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * user -> device -> gateway mapping.
 * HASH sess:{userId}   field=deviceId   value=gatewayId|connId|connectedAtMs
 * KEY  gw:{gatewayId}:alive  (TTL, refreshed by each gateway)
 */
public class SessionRegistry {

    public record Route(String deviceId, String gatewayId, String connId, long connectedAtMs) {
        static Route parse(String device, String raw) {
            String[] p = raw.split("\\|");
            return new Route(device, p[0], p[1], Long.parseLong(p[2]));
        }
    }

    /** Removes the device field only if it still belongs to THIS connection; returns remaining device count. */
    private static final RedisScript<Long> UNREGISTER = RedisScript.of("""
            local v = redis.call('HGET', KEYS[1], ARGV[1])
            if v and string.find(v, ARGV[2], 1, true) then redis.call('HDEL', KEYS[1], ARGV[1]) end
            return redis.call('HLEN', KEYS[1])
            """, Long.class);

    private static final Duration SESSION_TTL = Duration.ofSeconds(120);

    private final ReactiveStringRedisTemplate redis;
    private final PresenceStore presence;
    private final String gatewayId;

    public SessionRegistry(ReactiveStringRedisTemplate redis, PresenceStore presence, String gatewayId) {
        this.redis = redis;
        this.presence = presence;
        this.gatewayId = gatewayId;
    }

    public String gatewayId() { return gatewayId; }

    private static String key(long userId) { return "sess:" + userId; }

    public Mono<Void> register(long userId, String deviceId, String connId) {
        String v = gatewayId + "|" + connId + "|" + System.currentTimeMillis();
        return redis.<String, String>opsForHash().put(key(userId), deviceId, v)
                .then(redis.expire(key(userId), SESSION_TTL))
                .then(presence.online(userId, deviceId));
    }

    public Mono<Void> unregister(long userId, String deviceId, String connId) {
        return redis.execute(UNREGISTER, List.of(key(userId)), List.of(deviceId, "|" + connId + "|"))
                .next()
                .flatMap(remaining -> remaining == 0 ? presence.offline(userId) : Mono.<Void>empty());
    }

    public Flux<Route> routes(long userId) {
        return redis.<String, String>opsForHash().entries(key(userId))
                .map(e -> Route.parse(e.getKey(), e.getValue()));
    }

    /** Gateways batch-refresh TTLs for their live connections (not on every heartbeat). */
    public Mono<Void> touch(Flux<Long> userIds) {
        return userIds.flatMap(u -> redis.expire(key(u), SESSION_TTL), 256).then();
    }

    public Mono<Boolean> gatewayAlive(String gwId) {
        return redis.hasKey("gw:" + gwId + ":alive");
    }
}
