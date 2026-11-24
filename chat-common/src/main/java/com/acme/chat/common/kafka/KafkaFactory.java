package com.acme.chat.common.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public final class KafkaFactory {

    private KafkaFactory() { }

    /** Durable, idempotent producer: acks=all, lz4, 2 ms linger for micro-batching. */
    public static <V> KafkaSender<Long, V> sender(String bootstrap, ObjectMapper mapper) {
        Map<String, Object> p = new HashMap<>();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        SenderOptions<Long, V> options = SenderOptions.<Long, V>create(p)
                .withKeySerializer(new LongSerializer())
                .withValueSerializer(new JsonKafkaSerializer<V>(mapper));
        return KafkaSender.create(options);
    }

    /** Manual-ack consumer tuned for low latency (short fetch wait, frequent offset commits). */
    public static <V> KafkaReceiver<Long, V> receiver(String bootstrap, String groupId, Class<V> type,
                                                      ObjectMapper mapper, String... topics) {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 20);
        ReceiverOptions<Long, V> options = ReceiverOptions.<Long, V>create(p)
                .withKeyDeserializer(new LongDeserializer())
                .withValueDeserializer(new JsonKafkaDeserializer<V>(type, mapper))
                .subscription(List.of(topics))
                .commitInterval(Duration.ofMillis(200));
        return KafkaReceiver.create(options);
    }

    /** Sends one record and completes only after the broker acknowledged it (acks=all). */
    public static <V> Mono<Void> send(KafkaSender<Long, V> sender, String topic, long key, V value) {
        return sender.send(Mono.just(SenderRecord.create(new ProducerRecord<Long, V>(topic, key, value), key)))
                .next()
                .flatMap(r -> r.exception() != null ? Mono.<Void>error(r.exception()) : Mono.<Void>empty());
    }
}
