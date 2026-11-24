package com.acme.chat.domain;

import java.util.UUID;

public record SendCommand(UUID clientMsgId, long conversationId, MessageType type,
                          String body, String mediaRef, Long replyToId) { }
