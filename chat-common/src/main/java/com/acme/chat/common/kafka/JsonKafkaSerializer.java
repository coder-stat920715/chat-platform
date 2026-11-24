package com.acme.chat.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonKafkaSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper;

    public JsonKafkaSerializer(ObjectMapper mapper) { this.mapper = mapper; }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("serialize failed for topic " + topic, e);
        }
    }
}
