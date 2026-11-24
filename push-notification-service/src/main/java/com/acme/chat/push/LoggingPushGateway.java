package com.acme.chat.push;

import com.acme.chat.domain.PushPayload;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Dev stub: logs instead of calling FCM/APNs and can simulate transient failures. */
@Component
public class LoggingPushGateway implements PushGateway {

    private static final Logger log = LoggerFactory.getLogger(LoggingPushGateway.class);
    private final double failureRate;

    public LoggingPushGateway(@Value("${push.simulate-failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
    }

    @Override
    public void send(PushPayload p) {
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new PushExceptions.TransientPushException("simulated provider 503");
        }
        log.info("PUSH user={} conversation={} message={} collapseKey={}",
                p.userId(), p.conversationId(), p.messageId(), p.collapseKey());
    }
}
