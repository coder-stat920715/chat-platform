package com.acme.chat.gateway.ws;

import com.acme.chat.domain.WsFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Fan-out-on-read support for large channels: the gateway subscribes to Redis channel conv:{id}
 * only while at least one local connection has the channel open, and unsubscribes with the last one.
 */
@Component
public class ChannelSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(ChannelSubscriptions.class);

    private static final class Sub {
        final Set<String> connKeys = ConcurrentHashMap.newKeySet();
        Disposable redisSub;
    }

    private final ConcurrentHashMap<Long, Sub> subs = new ConcurrentHashMap<>();
    private final ReactiveRedisMessageListenerContainer listener;
    private final ConnectionRegistry connections;
    private final ObjectMapper mapper;

    public ChannelSubscriptions(ReactiveRedisMessageListenerContainer listener, ConnectionRegistry connections,
                                ObjectMapper mapper) {
        this.listener = listener;
        this.connections = connections;
        this.mapper = mapper;
    }

    public void subscribe(long conversationId, Connection c) {
        subs.compute(conversationId, (id, sub) -> {
            if (sub == null) {
                sub = new Sub();
                sub.redisSub = listener.receive(ChannelTopic.of("conv:" + id))
                        .map(ReactiveSubscription.Message::getMessage)
                        .subscribe(json -> fanout(id, json), e -> log.warn("channel sub {} failed: {}", id, e.toString()));
            }
            sub.connKeys.add(c.key());
            return sub;
        });
        c.channels.add(conversationId);
    }

    public void unsubscribe(long conversationId, Connection c) {
        c.channels.remove(conversationId);
        subs.compute(conversationId, (id, sub) -> {
            if (sub == null) return null;
            sub.connKeys.remove(c.key());
            if (sub.connKeys.isEmpty()) {
                sub.redisSub.dispose();
                return null;
            }
            return sub;
        });
    }

    public void unsubscribeAll(Connection c) {
        for (Long convId : c.channels) unsubscribe(convId, c);
    }

    private void fanout(long conversationId, String json) {
        Sub sub = subs.get(conversationId);
        if (sub == null) return;
        try {
            WsFrame frame = mapper.readValue(json, WsFrame.class);
            for (String key : sub.connKeys) connections.deliverByKey(key, frame);
        } catch (Exception e) {
            log.debug("bad channel frame for {}: {}", conversationId, e.toString());
        }
    }
}
