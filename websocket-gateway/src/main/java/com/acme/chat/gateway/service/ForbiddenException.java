package com.acme.chat.gateway.service;

public class ForbiddenException extends RuntimeException {
    public ForbiddenException(String message) { super(message); }
}
