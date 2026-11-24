package com.acme.chat.domain;

import java.time.Instant;
import java.util.List;

public record UserPresence(long userId, PresenceState state, Instant lastSeen, List<String> activeDevices) { }
