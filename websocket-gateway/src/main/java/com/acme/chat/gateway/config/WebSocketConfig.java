package com.acme.chat.gateway.config;

import com.acme.chat.gateway.ws.ChatWebSocketHandler;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;
import reactor.netty.http.server.WebsocketServerSpec;

@Configuration
public class WebSocketConfig {

    @Bean
    HandlerMapping webSocketMapping(ChatWebSocketHandler handler) {
        return new SimpleUrlHandlerMapping(Map.of("/ws/v1/chat", handler), -1);
    }

    @Bean
    WebSocketService webSocketService() {
        return new HandshakeWebSocketService(new ReactorNettyRequestUpgradeStrategy(
                () -> WebsocketServerSpec.builder().maxFramePayloadLength(64 * 1024)));
    }

    @Bean
    WebSocketHandlerAdapter webSocketHandlerAdapter(WebSocketService service) {
        return new WebSocketHandlerAdapter(service);
    }
}
