package com.acme.chat.common.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Returns null (and logs) on malformed payloads so one poison record can never kill a consumer. */
public class JsonKafkaDeserializer<T> implements Deserializer<T> {
    private static final Logger log = LoggerFactory.getLogger(JsonKafkaDeserializer.class);
    private final Class<T> type;
    private final ObjectMapper mapper;

    public JsonKafkaDeserializer(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return mapper.readValue(data, type);
        } catch (Exception e) {
            log.error("dropping undeserializable record on {}: {}", topic, e.toString());
            return null;
        }
    }
}
