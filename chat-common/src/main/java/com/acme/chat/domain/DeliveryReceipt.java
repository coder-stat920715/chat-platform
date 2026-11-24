package com.acme.chat.domain;

import java.time.Instant;

public record DeliveryReceipt(
        long conversationId,
        long messageId,
        long userId,
        String deviceId,
        MessageStatus status,
        boolean cumulative,
        Instant at) { }
