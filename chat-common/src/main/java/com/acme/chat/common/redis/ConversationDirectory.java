package com.acme.chat.common.redis;

import com.acme.chat.domain.Conversation;
import java.util.Collection;
import java.util.stream.Collectors;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Minimal Redis-backed membership/metadata directory used by this reference implementation.
 * In production this is a cache in front of group-chat-service (PostgreSQL).
 */
public class ConversationDirectory {

    private final ReactiveStringRedisTemplate redis;

    public ConversationDirectory(ReactiveStringRedisTemplate redis) { this.redis = redis; }

    private static String membersKey(long c) { return "conv:" + c + ":members"; }
    private static String metaKey(long c) { return "conv:" + c + ":meta"; }

    public Mono<Boolean> isMember(long conversationId, long userId) {
        return redis.opsForSet().isMember(membersKey(conversationId), Long.toString(userId));
    }

    public Flux<Long> members(long conversationId) {
        return redis.opsForSet().members(membersKey(conversationId)).map(Long::parseLong);
    }

    public Mono<Long> memberCount(long conversationId) {
        return redis.opsForSet().size(membersKey(conversationId));
    }

    public Mono<Boolean> isChannelMode(long conversationId) {
        return redis.<String, String>opsForHash().get(metaKey(conversationId), "channelMode")
                .map("true"::equals)
                .defaultIfEmpty(false);
    }

    /** Fan-out-on-write only for non-channel conversations up to FAN_OUT_WRITE_MAX members. */
    public Mono<Boolean> fanOutOnWrite(long conversationId) {
        return Mono.zip(isChannelMode(conversationId), memberCount(conversationId))
                .map(t -> !t.getT1() && t.getT2() <= Conversation.FAN_OUT_WRITE_MAX);
    }

    public Mono<Void> create(long conversationId, Collection<Long> memberIds, boolean channelMode) {
        String[] ids = memberIds.stream().map(String::valueOf).collect(Collectors.toList()).toArray(new String[0]);
        return redis.opsForSet().add(membersKey(conversationId), ids)
                .then(redis.<String, String>opsForHash().put(metaKey(conversationId), "channelMode",
                        Boolean.toString(channelMode)))
                .then();
    }
}