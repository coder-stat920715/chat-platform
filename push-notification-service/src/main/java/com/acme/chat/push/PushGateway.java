package com.acme.chat.push;

import com.acme.chat.domain.PushPayload;

/** Abstraction over FCM HTTP v1 / APNs HTTP/2. Implement with the Firebase Admin SDK or Pushy. */
public interface PushGateway {
    void send(PushPayload payload);
}
