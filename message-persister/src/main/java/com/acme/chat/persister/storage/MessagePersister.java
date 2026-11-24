package com.acme.chat.persister.storage;

import com.acme.chat.domain.ChatMessage;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.stereotype.Component;
import com.datastax.oss.driver.api.core.cql.BatchType;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

/**
 * Kafka -> Cassandra. Micro-batches records, groups them by partition key, and writes one UNLOGGED batch
 * per partition (cross-partition logged batches are an anti-pattern at this rate). Offsets are acknowledged
 * only after the write succeeded; replays are harmless because every write is an idempotent upsert.
 */
@Component
public class MessagePersister {

    private static final Logger log = LoggerFactory.getLogger(MessagePersister.class);
    private static final int BATCH_SIZE = 200;
    private static final Duration BATCH_WAIT = Duration.ofMillis(25);

    private final KafkaReceiver<Long, ChatMessage> receiver;
    private final ReactiveCassandraTemplate cassandra;
    private final ReactiveCqlOperations cql;
    private Disposable subscription;

    public MessagePersister(KafkaReceiver<Long, ChatMessage> persisterMessageReceiver,
                            ReactiveCassandraTemplate cassandra) {
        this.receiver = persisterMessageReceiver;
        this.cassandra = cassandra;
        this.cql = cassandra.getReactiveCqlOperations();
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        subscription = receiver.receive()
                .filter(r -> r.value() != null)
                .bufferTimeout(BATCH_SIZE, BATCH_WAIT, true)      // fair backpressure
                .concatMap(this::persistBatch, 1)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(10)))
                .subscribe(v -> { }, e -> log.error("persister pipeline died", e));
        log.info("message persister started");
    }

    @PreDestroy
    void stop() {
        if (subscription != null) subscription.dispose();
    }

    private Mono<Void> persistBatch(List<ReceiverRecord<Long, ChatMessage>> records) {
        Map<MessageKey, List<MessageEntity>> byPartition = records.stream()
                .map(r -> MessageEntity.from(r.value()))
                .collect(Collectors.groupingBy(e ->
                        new MessageKey(e.getKey().getConversationId(), e.getKey().getBucketDate(), 0L)));

        return Flux.fromIterable(byPartition.entrySet())
                .flatMap(e -> cassandra.batchOps(BatchType.UNLOGGED).insert(e.getValue()).execute()
                        .then(touchBucket(e.getKey())), 32)
                .then()
                .doOnSuccess(v -> records.forEach(r -> r.receiverOffset().acknowledge()));
    }

    /** Lets history paging skip empty days. Idempotent. */
    private Mono<Void> touchBucket(MessageKey partition) {
        return cql.execute("INSERT INTO conversation_buckets (conversation_id, bucket_date) VALUES (?, ?)",
                partition.getConversationId(), partition.getBucketDate()).then();
    }
}
