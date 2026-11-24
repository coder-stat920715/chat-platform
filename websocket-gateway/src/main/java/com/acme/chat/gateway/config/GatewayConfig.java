package com.acme.chat.gateway.config;

import com.acme.chat.common.id.SnowflakeIdGenerator;
import com.acme.chat.common.kafka.KafkaFactory;
import com.acme.chat.common.redis.ConversationDirectory;
import com.acme.chat.common.redis.PresenceStore;
import com.acme.chat.common.redis.SessionRegistry;
import com.acme.chat.domain.ChatEvent;
import com.acme.chat.domain.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import reactor.kafka.sender.KafkaSender;

@Configuration
public class GatewayConfig {

    @Bean
    SnowflakeIdGenerator snowflake(@Value("${chat.worker-id}") long workerId) {
        return new SnowflakeIdGenerator(workerId);
    }

    @Bean
    ConversationDirectory conversationDirectory(ReactiveStringRedisTemplate redis) {
        return new ConversationDirectory(redis);
    }

    @Bean
    PresenceStore presenceStore(ReactiveStringRedisTemplate redis) {
        return new PresenceStore(redis);
    }

    @Bean
    SessionRegistry sessionRegistry(ReactiveStringRedisTemplate redis, PresenceStore presence,
                                    @Value("${chat.gateway-id}") String gatewayId) {
        return new SessionRegistry(redis, presence, gatewayId);
    }

    @Bean
    KafkaSender<Long, ChatMessage> messageSender(@Value("${chat.kafka.bootstrap-servers}") String bootstrap,
                                                 ObjectMapper mapper) {
        return KafkaFactory.sender(bootstrap, mapper);
    }

    @Bean
    KafkaSender<Long, ChatEvent> eventSender(@Value("${chat.kafka.bootstrap-servers}") String bootstrap,
                                             ObjectMapper mapper) {
        return KafkaFactory.sender(bootstrap, mapper);
    }

    @Bean(destroyMethod = "destroy")
    ReactiveRedisMessageListenerContainer redisListener(ReactiveRedisConnectionFactory factory) {
        return new ReactiveRedisMessageListenerContainer(factory);
    }
}
