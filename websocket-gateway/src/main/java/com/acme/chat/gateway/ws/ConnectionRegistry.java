package com.acme.chat.gateway.ws;

import com.acme.chat.domain.WsFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketMessage;
import reactor.core.publisher.Sinks;

/** Thread-safe, per-node registry of live sockets. */
@Component
public class ConnectionRegistry {

    private final ConcurrentHashMap<String, Connection> byKey = new ConcurrentHashMap<>();
    private final ObjectMapper mapper;

    public ConnectionRegistry(ObjectMapper mapper) { this.mapper = mapper; }

    /** Returns the previous connection for the same user+device (caller closes it: last writer wins). */
    Connection put(Connection c) { return byKey.put(c.key(), c); }

    void remove(Connection c) { byKey.remove(c.key(), c); }

    public int size() { return byKey.size(); }

    public Stream<Long> userIds() { return byKey.values().stream().map(c -> c.userId).distinct(); }

    public boolean deliver(long userId, String deviceId, WsFrame frame) {
        return deliverByKey(userId + ":" + deviceId, frame);
    }

    public boolean deliverByKey(String connKey, WsFrame frame) {
        Connection c = byKey.get(connKey);
        return c != null && send(c, frame);
    }

    public int deliverToUser(long userId, List<String> deviceIds, WsFrame frame) {
        int ok = 0;
        for (String d : deviceIds) if (deliver(userId, d, frame)) ok++;
        return ok;
    }

    boolean send(Connection c, WsFrame frame) {
        WebSocketMessage msg = null;
        try {
            msg = c.session.textMessage(mapper.writeValueAsString(frame));
            Sinks.EmitResult r = emit(c.outbound, msg);
            if (r.isSuccess()) return true;
            DataBufferUtils.release(msg.getPayload());
            if (r == Sinks.EmitResult.FAIL_OVERFLOW) {
                // Slow consumer: cut it loose; the client reconnects and resyncs from inbox/history.
                c.session.close(CloseStatus.POLICY_VIOLATION.withReason("slow-consumer")).subscribe();
            }
            return false;
        } catch (Exception e) {
            if (msg != null) DataBufferUtils.release(msg.getPayload());
            return false;
        }
    }

    /** tryEmitNext can report FAIL_NON_SERIALIZED under concurrent producers; retry briefly. */
    private static Sinks.EmitResult emit(Sinks.Many<WebSocketMessage> sink, WebSocketMessage m) {
        Sinks.EmitResult r;
        int spins = 0;
        do {
            r = sink.tryEmitNext(m);
        } while (r == Sinks.EmitResult.FAIL_NON_SERIALIZED && ++spins < 100);
        return r;
    }
}
