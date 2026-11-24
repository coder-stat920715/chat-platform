package com.acme.chat.domain;

import com.fasterxml.jackson.databind.JsonNode;

/** Ephemeral/aux events on chat.events.v1: RECEIPT and TYPING. Key = conversationId. */
public record ChatEvent(String type, long conversationId, long actorId, JsonNode payload) { }
