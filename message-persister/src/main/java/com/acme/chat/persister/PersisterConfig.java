package com.acme.chat.persister;

import com.acme.chat.common.kafka.KafkaFactory;
import com.acme.chat.common.kafka.Topics;
import com.acme.chat.domain.ChatEvent;
import com.acme.chat.domain.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;

@Configuration
public class PersisterConfig {

    @Bean
    KafkaReceiver<Long, ChatMessage> persisterMessageReceiver(@Value("${chat.kafka.bootstrap-servers}") String b,
                                                              ObjectMapper mapper) {
        return KafkaFactory.receiver(b, "persister", ChatMessage.class, mapper, Topics.MESSAGES);
    }

    @Bean
    KafkaReceiver<Long, ChatEvent> persisterEventReceiver(@Value("${chat.kafka.bootstrap-servers}") String b,
                                                          ObjectMapper mapper) {
        return KafkaFactory.receiver(b, "persister-events", ChatEvent.class, mapper, Topics.EVENTS);
    }
}
