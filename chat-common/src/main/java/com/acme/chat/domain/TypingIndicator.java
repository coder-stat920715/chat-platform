package com.acme.chat.domain;

import java.time.Instant;

public record TypingIndicator(long conversationId, long userId, boolean typing, Instant at) { }
