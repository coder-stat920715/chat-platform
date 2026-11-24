package com.acme.chat.push;

import com.acme.chat.common.kafka.Topics;
import com.acme.chat.domain.PushPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Non-blocking retries: failed records hop through push.notifications.v1-retry-0/1/2 (5 s, 30 s, 3 min),
 * so a slow provider never stalls the main partition. Exhausted records land in the -dlt topic.
 * A push is only a wake-up hint: the message itself is already durable in Cassandra and the offline inbox.
 */
@Component
public class PushNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(PushNotificationListener.class);

    private final PushGateway gateway;

    public PushNotificationListener(PushGateway gateway) { this.gateway = gateway; }

    @RetryableTopic(
            attempts = "4",                                    // 1 original + 3 retries
            backoff = @Backoff(delay = 5_000, multiplier = 6.0, maxDelay = 300_000),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = PushExceptions.TransientPushException.class)
    @KafkaListener(topics = Topics.PUSH, groupId = "push-sender", concurrency = "4")
    public void onPush(PushPayload payload) {
        try {
            gateway.send(payload);
        } catch (PushExceptions.InvalidTokenException e) {
            // Permanent: do not retry, do not dead-letter. Revoke the token in the token store here.
            log.warn("invalid push token for user {}, revoking", payload.userId());
        }
        // TransientPushException propagates -> next retry topic with backoff.
    }

    @DltHandler
    public void onDead(PushPayload payload, @Header(KafkaHeaders.EXCEPTION_MESSAGE) String reason) {
        // Persist to a dead_push table / alert here. Delivery is still guaranteed via the offline inbox.
        log.error("push dead-lettered user={} message={} reason={}", payload.userId(), payload.messageId(), reason);
    }
}
