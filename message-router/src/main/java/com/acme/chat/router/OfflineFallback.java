package com.acme.chat.router;

import com.acme.chat.common.kafka.KafkaFactory;
import com.acme.chat.common.kafka.Topics;
import com.acme.chat.domain.ChatMessage;
import com.acme.chat.domain.PushPayload;
import java.util.Set;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

/**
 * Recipients without a live socket: (1) durable offline inbox row in Cassandra (30 d TTL via table default),
 * (2) a push wake-up on push.notifications.v1. A push failure never loses a message: the inbox is the truth.
 */
@Component
public class OfflineFallback {

    private final KafkaSender<Long, PushPayload> pushSender;
    private final ReactiveCqlOperations cql;
    private final ReactiveStringRedisTemplate redis;

    public OfflineFallback(KafkaSender<Long, PushPayload> pushSender, ReactiveCassandraTemplate cassandra,
                           ReactiveStringRedisTemplate redis) {
        this.pushSender = pushSender;
        this.cql = cassandra.getReactiveCqlOperations();
        this.redis = redis;
    }

    public Mono<Void> handle(ChatMessage m, Set<Long> offlineUsers) {
        return Flux.fromIterable(offlineUsers)
                .flatMap(uid -> buffer(uid, m).then(Mono.defer(() -> pushUnlessMuted(uid, m))), 64)
                .then();
    }

    private Mono<Void> buffer(long userId, ChatMessage m) {
        return cql.execute("INSERT INTO user_inbox (user_id, message_id, conversation_id) VALUES (?, ?, ?)",
                userId, m.messageId(), m.conversationId()).then();
    }

    private Mono<Void> pushUnlessMuted(long userId, ChatMessage m) {
        return redis.opsForSet().isMember("mute:" + userId, Long.toString(m.conversationId()))
                .defaultIfEmpty(false)
                .flatMap(muted -> muted ? Mono.<Void>empty() : KafkaFactory.send(pushSender, Topics.PUSH, userId,
                        new PushPayload(userId, m.conversationId(), m.messageId(), "", "",
                                "conv-" + m.conversationId(), 10)));
    }
}
