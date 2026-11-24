package com.acme.chat.common.kafka;

import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public final class KafkaPipelines {

    private static final Logger log = LoggerFactory.getLogger(KafkaPipelines.class);

    private KafkaPipelines() { }

    /**
     * Preserves per-partition order (concatMap inside each partition group) while processing partitions
     * in parallel. A failing handler is logged and skipped so a poison record never stalls a partition;
     * the offset is acknowledged after the handler finishes.
     */
    public static <K, V> Flux<Void> perPartition(Flux<ReceiverRecord<K, V>> in,
                                                 Function<ReceiverRecord<K, V>, Mono<Void>> handler) {
        return in.groupBy(r -> r.receiverOffset().topicPartition())
                .flatMap(group -> group.concatMap(r -> {
                    Mono<Void> work = r.value() == null
                            ? Mono.<Void>empty()
                            : handler.apply(r).onErrorResume(e -> {
                                log.error("handler failed for {}-{}@{}: {}", r.topic(), r.partition(),
                                        r.offset(), e.toString());
                                return Mono.<Void>empty();
                            });
                    return work.then(Mono.<Void>fromRunnable(() -> r.receiverOffset().acknowledge()));
                }), 1024);
    }
}
