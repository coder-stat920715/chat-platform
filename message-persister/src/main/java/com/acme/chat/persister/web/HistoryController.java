package com.acme.chat.persister.web;

import com.acme.chat.domain.MessageType;
import com.acme.chat.persister.storage.HistoryService;
import com.acme.chat.persister.storage.MessageEntity;
import com.acme.chat.common.id.SnowflakeIdGenerator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/conversations")
public class HistoryController {

    public record MessageDto(long messageId, long senderId, MessageType type, String body, String mediaRef,
                             long serverTimeMs) {
        static MessageDto from(MessageEntity e) {
            long id = e.getKey().getMessageId();
            return new MessageDto(id, e.getSenderId(), MessageType.values()[e.getType()], e.getBody(),
                    e.getMediaRef(), SnowflakeIdGenerator.extractTimestampMs(id));
        }
    }

    private final HistoryService history;

    public HistoryController(HistoryService history) { this.history = history; }

    /** GET /api/conversations/{id}/messages?before={messageId}&limit=50 (newest first). */
    @GetMapping("/{conversationId}/messages")
    public Flux<MessageDto> messages(@PathVariable long conversationId,
                                     @RequestParam(defaultValue = "0") long before,
                                     @RequestParam(defaultValue = "50") int limit) {
        // TODO: authenticate caller and verify membership via group-chat-service before returning data.
        return history.page(conversationId, before, limit).map(MessageDto::from);
    }
}
