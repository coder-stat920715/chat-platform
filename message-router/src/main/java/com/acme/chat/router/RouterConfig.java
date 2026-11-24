package com.acme.chat.router;

import com.acme.chat.common.kafka.KafkaFactory;
import com.acme.chat.common.kafka.Topics;
import com.acme.chat.common.redis.ConversationDirectory;
import com.acme.chat.common.redis.PresenceStore;
import com.acme.chat.common.redis.SessionRegistry;
import com.acme.chat.domain.ChatEvent;
import com.acme.chat.domain.ChatMessage;
import com.acme.chat.domain.PushPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

@Configuration
public class RouterConfig {

    @Bean
    ConversationDirectory conversationDirectory(ReactiveStringRedisTemplate redis) {
        return new ConversationDirectory(redis);
    }

    @Bean
    PresenceStore presenceStore(ReactiveStringRedisTemplate redis) {
        return new PresenceStore(redis);
    }

    /** The router only reads routes; its own gateway id is irrelevant. */
    @Bean
    SessionRegistry sessionRegistry(ReactiveStringRedisTemplate redis, PresenceStore presence) {
        return new SessionRegistry(redis, presence, "router");
    }

    @Bean
    KafkaReceiver<Long, ChatMessage> messageReceiver(@Value("${chat.kafka.bootstrap-servers}") String b,
                                                     ObjectMapper mapper) {
        return KafkaFactory.receiver(b, "router", ChatMessage.class, mapper, Topics.MESSAGES);
    }

    @Bean
    KafkaReceiver<Long, ChatEvent> eventReceiver(@Value("${chat.kafka.bootstrap-servers}") String b,
                                                 ObjectMapper mapper) {
        return KafkaFactory.receiver(b, "router-events", ChatEvent.class, mapper, Topics.EVENTS);
    }

    @Bean
    KafkaSender<Long, PushPayload> pushSender(@Value("${chat.kafka.bootstrap-servers}") String b,
                                              ObjectMapper mapper) {
        return KafkaFactory.sender(b, mapper);
    }
}
