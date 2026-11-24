package com.acme.chat.gateway.web;

import com.acme.chat.common.redis.ConversationDirectory;
import com.acme.chat.common.redis.PresenceStore;
import com.acme.chat.domain.UserPresence;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/** Dev helpers standing in for group-chat-service / presence-service. Remove or secure in production. */
@RestController
@RequestMapping("/dev")
public class DevController {

    public record CreateConversation(long conversationId, List<Long> members, boolean channelMode) { }

    private final ConversationDirectory directory;
    private final PresenceStore presence;

    public DevController(ConversationDirectory directory, PresenceStore presence) {
        this.directory = directory;
        this.presence = presence;
    }

    @PostMapping("/conversations")
    public Mono<Void> create(@RequestBody CreateConversation req) {
        return directory.create(req.conversationId(), req.members(), req.channelMode());
    }

    @GetMapping("/presence/{userId}")
    public Mono<UserPresence> presence(@PathVariable long userId) {
        return presence.get(userId);
    }
}
