package com.acme.chat.domain;

/** Router -> gateway instruction carried over Redis Pub/Sub (channel gw:{gatewayId}). */
public record DeliverEnvelope(long userId, String deviceId, String connId, WsFrame frame) { }
