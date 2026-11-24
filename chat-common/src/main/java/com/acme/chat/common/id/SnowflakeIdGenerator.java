package com.acme.chat.common.id;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.function.LongSupplier;

/**
 * 64-bit time-sortable ids: 1 sign | 41 timestamp (ms since custom epoch) | 10 worker | 12 sequence.
 * 4096 ids/ms/worker, ~69 years of range.
 */
public final class SnowflakeIdGenerator {

    private static final long EPOCH_MS = 1_735_689_600_000L; // 2025-01-01T00:00:00Z
    private static final int WORKER_BITS = 10;
    private static final int SEQUENCE_BITS = 12;
    private static final long MAX_WORKER_ID = (1L << WORKER_BITS) - 1;
    private static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1;
    private static final int WORKER_SHIFT = SEQUENCE_BITS;
    private static final int TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_BITS;
    private static final long MAX_BACKWARD_WAIT_MS = 5;

    private final long workerId;
    private final LongSupplier clock;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long workerId) {
        this(workerId, System::currentTimeMillis);
    }

    public SnowflakeIdGenerator(long workerId, LongSupplier clock) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("workerId must be in [0, " + MAX_WORKER_ID + "]");
        }
        this.workerId = workerId;
        this.clock = clock;
    }

    /** Thread-safe; the critical section is nanoseconds, negligible next to network I/O. */
    public synchronized long nextId() {
        long now = clock.getAsLong();

        if (now < lastTimestamp) {
            long drift = lastTimestamp - now;
            if (drift > MAX_BACKWARD_WAIT_MS) {
                throw new IllegalStateException("Clock moved backwards by " + drift + " ms");
            }
            now = waitUntil(lastTimestamp);
        }

        if (now == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                now = waitUntil(lastTimestamp + 1);
            }
        } else {
            sequence = 0;
        }

        lastTimestamp = now;
        return ((now - EPOCH_MS) << TIMESTAMP_SHIFT) | (workerId << WORKER_SHIFT) | sequence;
    }

    private long waitUntil(long target) {
        long t = clock.getAsLong();
        while (t < target) {
            Thread.onSpinWait();
            t = clock.getAsLong();
        }
        return t;
    }

    public static long extractTimestampMs(long id) {
        return (id >>> TIMESTAMP_SHIFT) + EPOCH_MS;
    }

    /** yyyymmdd (UTC): the Cassandra partition bucket of an id. */
    public static int bucketDate(long id) {
        return bucketDateOfMillis(extractTimestampMs(id));
    }

    public static int bucketDateOfMillis(long epochMs) {
        LocalDate d = Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).toLocalDate();
        return d.getYear() * 10_000 + d.getMonthValue() * 100 + d.getDayOfMonth();
    }
}
