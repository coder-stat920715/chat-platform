package com.acme.chat.domain;

public record PushPayload(
        long userId,
        long conversationId,
        long messageId,
        String senderName,
        String preview,
        String collapseKey,
        int priority) { }
