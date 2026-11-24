package com.acme.chat.domain;

import com.fasterxml.jackson.databind.JsonNode;

/** Wire envelope (JSON over WebSocket). Swap for Protobuf in production. */
public record WsFrame(String type, String correlationId, JsonNode payload) {
    public static final String SEND = "SEND";
    public static final String ACK = "ACK";
    public static final String MESSAGE = "MESSAGE";
    public static final String RECEIPT = "RECEIPT";
    public static final String TYPING = "TYPING";
    public static final String SUBSCRIBE = "SUBSCRIBE";
    public static final String UNSUBSCRIBE = "UNSUBSCRIBE";
    public static final String PING = "PING";
    public static final String PONG = "PONG";
    public static final String ERROR = "ERROR";
}
