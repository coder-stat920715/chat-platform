package com.acme.chat.domain;

import java.util.UUID;

public record Ack(UUID clientMsgId, long messageId, long serverTimeMs) { }
