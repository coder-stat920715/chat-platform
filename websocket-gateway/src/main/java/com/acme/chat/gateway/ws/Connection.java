package com.acme.chat.gateway.ws;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Sinks;

/** One live socket. All outbound writes go through the bounded sink (single writer, no races). */
public final class Connection {
    final long userId;
    final String deviceId;
    final WebSocketSession session;
    final Sinks.Many<WebSocketMessage> outbound;
    final AtomicLong lastSeenMs = new AtomicLong(System.currentTimeMillis());
    final Set<Long> channels = ConcurrentHashMap.newKeySet();

    Connection(long userId, String deviceId, WebSocketSession session, Sinks.Many<WebSocketMessage> outbound) {
        this.userId = userId;
        this.deviceId = deviceId;
        this.session = session;
        this.outbound = outbound;
    }

    public String connId() { return session.getId(); }
    public String key() { return userId + ":" + deviceId; }
}
