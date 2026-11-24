package com.acme.chat.gateway.service;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** Fixed-window per-user limiter in Redis (swap for a Lua token bucket for smoother behavior). */
@Component
public class RateLimiter {

    private final ReactiveStringRedisTemplate redis;
    private final long limitPerSecond;

    public RateLimiter(ReactiveStringRedisTemplate redis,
                       @Value("${chat.rate-limit-per-second:40}") long limitPerSecond) {
        this.redis = redis;
        this.limitPerSecond = limitPerSecond;
    }

    public Mono<Boolean> tryAcquire(long userId) {
        String k = "rl:" + userId + ":" + (System.currentTimeMillis() / 1000);
        return redis.opsForValue().increment(k)
                .flatMap(n -> n == 1L ? redis.expire(k, Duration.ofSeconds(2)).thenReturn(n) : Mono.just(n))
                .map(n -> n <= limitPerSecond);
    }
}
