package com.acme.chat.common.redis;

import com.acme.chat.domain.PresenceState;
import com.acme.chat.domain.UserPresence;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

public class PresenceStore {

    private final ReactiveStringRedisTemplate redis;

    public PresenceStore(ReactiveStringRedisTemplate redis) { this.redis = redis; }

    private static String key(long userId) { return "presence:" + userId; }

    public Mono<Void> online(long userId, String deviceId) {
        return write(userId, "ONLINE");
    }

    public Mono<Void> offline(long userId) {
        return write(userId, "OFFLINE");
    }

    private Mono<Void> write(long userId, String state) {
        return redis.<String, String>opsForHash()
                .putAll(key(userId), Map.of("state", state, "lastSeen", Long.toString(System.currentTimeMillis())))
                .then();
    }

    /** ONLINE if at least one live device session exists (derived from the session registry). */
    public Mono<UserPresence> get(long userId) {
        Mono<Map<String, String>> stored = redis.<String, String>opsForHash().entries(key(userId))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
        Mono<List<String>> devices = redis.<String, String>opsForHash().keys("sess:" + userId).collectList();
        return Mono.zip(stored, devices).map(t -> {
            long lastSeen = Long.parseLong(t.getT1().getOrDefault("lastSeen", "0"));
            PresenceState state = t.getT2().isEmpty() ? PresenceState.OFFLINE : PresenceState.ONLINE;
            return new UserPresence(userId, state, Instant.ofEpochMilli(lastSeen), t.getT2());
        });
    }
}
