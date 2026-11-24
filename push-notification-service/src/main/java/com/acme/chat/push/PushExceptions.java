package com.acme.chat.push;

public final class PushExceptions {
    private PushExceptions() { }

    /** 5xx / 429 / timeouts: retried with exponential backoff, then dead-lettered. */
    public static class TransientPushException extends RuntimeException {
        public TransientPushException(String m) { super(m); }
    }

    /** Device token no longer valid (FCM UNREGISTERED / APNs 410): never retried. */
    public static class InvalidTokenException extends RuntimeException {
        public InvalidTokenException(String m) { super(m); }
    }
}
