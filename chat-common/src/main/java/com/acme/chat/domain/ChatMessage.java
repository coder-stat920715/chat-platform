package com.acme.chat.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.UUID;

/** Canonical accepted message; also the Kafka value of chat.messages.v1. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ChatMessage(
        long messageId,
        UUID clientMsgId,
        long conversationId,
        long senderId,
        String senderDeviceId,
        MessageType type,
        String body,
        String mediaRef,
        Long replyToId,
        Instant serverTime,
        int schemaVersion) {

    public static final int MAX_BODY = 16_384;

    public ChatMessage {
        if (body != null && body.length() > MAX_BODY) {
            throw new IllegalArgumentException("body too large");
        }
    }
}
