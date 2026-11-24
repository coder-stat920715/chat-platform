package com.acme.chat.router;

import com.acme.chat.common.kafka.KafkaPipelines;
import com.acme.chat.common.redis.ConversationDirectory;
import com.acme.chat.domain.ChatEvent;
import com.acme.chat.domain.ChatMessage;
import com.acme.chat.domain.WsFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.util.retry.Retry;

/** Kafka -> routing. Ordering per conversation is preserved by partitioned, sequential processing. */
@Component
public class DispatchConsumers {

    private static final Logger log = LoggerFactory.getLogger(DispatchConsumers.class);

    private final KafkaReceiver<Long, ChatMessage> messageReceiver;
    private final KafkaReceiver<Long, ChatEvent> eventReceiver;
    private final ConversationDirectory directory;
    private final RedisConnectionRouter router;
    private final OfflineFallback offline;
    private final ObjectMapper mapper;
    private Disposable messages;
    private Disposable events;

    public DispatchConsumers(KafkaReceiver<Long, ChatMessage> messageReceiver,
                             KafkaReceiver<Long, ChatEvent> eventReceiver, ConversationDirectory directory,
                             RedisConnectionRouter router, OfflineFallback offline, ObjectMapper mapper) {
        this.messageReceiver = messageReceiver;
        this.eventReceiver = eventReceiver;
        this.directory = directory;
        this.router = router;
        this.offline = offline;
        this.mapper = mapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        Retry forever = Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(10));
        messages = KafkaPipelines.perPartition(messageReceiver.receive(), r -> routeMessage(r.value()))
                .retryWhen(forever).subscribe(v -> { }, e -> log.error("message pipeline died", e));
        events = KafkaPipelines.perPartition(eventReceiver.receive(), r -> routeEvent(r.value()))
                .retryWhen(forever).subscribe(v -> { }, e -> log.error("event pipeline died", e));
        log.info("router consumers started");
    }

    @PreDestroy
    void stop() {
        if (messages != null) messages.dispose();
        if (events != null) events.dispose();
    }

    private Mono<Void> routeMessage(ChatMessage m) {
        WsFrame frame = new WsFrame(WsFrame.MESSAGE, null, mapper.valueToTree(m));
        return directory.fanOutOnWrite(m.conversationId()).flatMap(writeFanOut -> {
            if (!writeFanOut) {
                // Large channel: ONE publish; only gateways with the channel open receive it.
                return router.publishToChannel(m.conversationId(), frame).then();
            }
            return directory.members(m.conversationId()).collectList()
                    .flatMap(members -> router.route(frame, members, m.senderId(), m.senderDeviceId()))
                    .flatMap(result -> {
                        var offlineUsers = new java.util.HashSet<>(result.offline());
                        offlineUsers.remove(m.senderId());   // sender having no OTHER device is not "offline"
                        return offline.handle(m, offlineUsers);
                    });
        });
    }

    private Mono<Void> routeEvent(ChatEvent e) {
        return directory.isChannelMode(e.conversationId()).flatMap(channel -> {
            if (channel) return Mono.<Void>empty();     // no typing/receipt storms in huge channels
            WsFrame frame = new WsFrame(e.type(), null, e.payload());
            return directory.members(e.conversationId())
                    .filter(u -> u != e.actorId())
                    .collectList()
                    .flatMap(members -> router.route(frame, members, -1, ""))
                    .then();
        });
    }
}
