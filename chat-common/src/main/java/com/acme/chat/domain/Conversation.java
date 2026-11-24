package com.acme.chat.domain;

import java.time.Instant;

public record Conversation(
        long conversationId,
        ConversationType type,
        String title,
        long ownerId,
        int memberCount,
        boolean channelMode,
        Instant createdAt) {

    public static final int FAN_OUT_WRITE_MAX = 500;

    public boolean fanOutOnWrite() {
        return !channelMode && memberCount <= FAN_OUT_WRITE_MAX;
    }
}
