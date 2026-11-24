package com.acme.chat.gateway.service;

import com.acme.chat.common.id.SnowflakeIdGenerator;
import com.acme.chat.common.kafka.KafkaFactory;
import com.acme.chat.common.kafka.Topics;
import com.acme.chat.common.redis.ConversationDirectory;
import com.acme.chat.domain.Ack;
import com.acme.chat.domain.ChatMessage;
import com.acme.chat.domain.SendCommand;
import java.time.Duration;
import java.time.Instant;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

/**
 * Accepts a message: authorize -> dedupe -> assign Snowflake id -> produce to Kafka (acks=all).
 * The SENT ack is returned only after Kafka has durably acknowledged the record.
 */
@Service
public class MessageIngressService {

    private final SnowflakeIdGenerator ids;
    private final KafkaSender<Long, ChatMessage> kafka;
    private final ReactiveStringRedisTemplate redis;
    private final ConversationDirectory directory;

    public MessageIngressService(SnowflakeIdGenerator ids, KafkaSender<Long, ChatMessage> kafka,
                                 ReactiveStringRedisTemplate redis, ConversationDirectory directory) {
        this.ids = ids;
        this.kafka = kafka;
        this.redis = redis;
        this.directory = directory;
    }

    public Mono<Ack> ingest(long senderId, String deviceId, SendCommand cmd) {
        if (cmd.clientMsgId() == null || cmd.type() == null) {
            return Mono.error(new IllegalArgumentException("clientMsgId and type are required"));
        }
        return directory.isMember(cmd.conversationId(), senderId)
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new ForbiddenException("not a member")))
                .flatMap(ok -> {
                    long candidateId = ids.nextId();
                    String dedupKey = "dedup:" + senderId + ":" + cmd.clientMsgId();
                    return redis.opsForValue()
                            .setIfAbsent(dedupKey, Long.toString(candidateId), Duration.ofMinutes(10))
                            .flatMap(first -> first
                                    ? produce(candidateId, senderId, deviceId, cmd)
                                            // allow the client to retry if the broker rejected the write
                                            .onErrorResume(e -> redis.delete(dedupKey).then(Mono.<Ack>error(e)))
                                    : redis.opsForValue().get(dedupKey)
                                            .map(v -> new Ack(cmd.clientMsgId(), Long.parseLong(v),
                                                    System.currentTimeMillis())));
                });
    }

    private Mono<Ack> produce(long messageId, long senderId, String deviceId, SendCommand cmd) {
        ChatMessage msg = new ChatMessage(messageId, cmd.clientMsgId(), cmd.conversationId(), senderId, deviceId,
                cmd.type(), cmd.body(), cmd.mediaRef(), cmd.replyToId(), Instant.now(), 1);
        return KafkaFactory.send(kafka, Topics.MESSAGES, msg.conversationId(), msg)   // key = conversationId
                .timeout(Duration.ofSeconds(2))
                .thenReturn(new Ack(cmd.clientMsgId(), messageId, msg.serverTime().toEpochMilli()));
    }
}
