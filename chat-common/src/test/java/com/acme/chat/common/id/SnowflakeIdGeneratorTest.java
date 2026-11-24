package com.acme.chat.common.id;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SnowflakeIdGeneratorTest {

    @Test
    void idsAreUniqueAndStrictlyIncreasing() {
        var gen = new SnowflakeIdGenerator(7);
        Set<Long> seen = new HashSet<>();
        long prev = -1;
        for (int i = 0; i < 100_000; i++) {
            long id = gen.nextId();
            assertTrue(id > prev, "ids must be increasing");
            assertTrue(seen.add(id), "ids must be unique");
            prev = id;
        }
    }

    @Test
    void timestampRoundTrips() {
        long now = System.currentTimeMillis();
        var gen = new SnowflakeIdGenerator(1, () -> now);
        assertEquals(now, SnowflakeIdGenerator.extractTimestampMs(gen.nextId()));
    }

    @Test
    void largeBackwardClockJumpIsRejected() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        var gen = new SnowflakeIdGenerator(1, clock::get);
        gen.nextId();
        clock.addAndGet(-100);
        assertThrows(IllegalStateException.class, gen::nextId);
    }

    @Test
    void invalidWorkerIdRejected() {
        assertThrows(IllegalArgumentException.class, () -> new SnowflakeIdGenerator(1024));
    }

    @Test
    void bucketDateFormat() {
        // 2026-01-15T00:00:00Z
        assertEquals(20260115, SnowflakeIdGenerator.bucketDateOfMillis(1_768_435_200_000L));
    }
}
