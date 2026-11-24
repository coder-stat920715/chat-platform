package com.acme.chat.persister.storage;

import com.acme.chat.common.kafka.KafkaPipelines;
import com.acme.chat.domain.ChatEvent;
import com.acme.chat.domain.DeliveryReceipt;
import com.acme.chat.domain.MessageStatus;
import com.acme.chat.domain.WsFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.util.retry.Retry;

/**
 * Persists read/delivered state as per-user cursors (one row per user per conversation, not per message).
 * USING TIMESTAMP = messageId (Snowflake, monotonic) makes last-write-wins act as "max()", so a late or
 * replayed older receipt can never move a cursor backwards.
 */
@Component
public class EventPersister {

    private static final Logger log = LoggerFactory.getLogger(EventPersister.class);

    private final KafkaReceiver<Long, ChatEvent> receiver;
    private final ReactiveCqlOperations cql;
    private final ObjectMapper mapper;
    private Disposable subscription;

    public EventPersister(KafkaReceiver<Long, ChatEvent> persisterEventReceiver, ReactiveCassandraTemplate cassandra,
                          ObjectMapper mapper) {
        this.receiver = persisterEventReceiver;
        this.cql = cassandra.getReactiveCqlOperations();
        this.mapper = mapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        subscription = KafkaPipelines.perPartition(receiver.receive(), r -> handle(r.value()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(10)))
                .subscribe(v -> { }, e -> log.error("event persister died", e));
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }

    private Mono<Void> handle(ChatEvent e) {
        if (!WsFrame.RECEIPT.equals(e.type())) return Mono.empty();     // typing is ephemeral
        DeliveryReceipt r = mapper.convertValue(e.payload(), DeliveryReceipt.class);
        String column = r.status() == MessageStatus.READ ? "last_read_id" : "last_delivered_id";
        return cql.execute("UPDATE read_cursors USING TIMESTAMP ? SET " + column
                        + " = ? WHERE user_id = ? AND conversation_id = ?",
                r.messageId(), r.messageId(), r.userId(), r.conversationId()).then();
    }
}
