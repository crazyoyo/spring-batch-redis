package org.springframework.batch.item.redis.support;

import java.time.Duration;
import java.time.Instant;

public class Timer {

    private final Instant start = Instant.now();
    private final Duration timeout;

    public Timer(Duration timeout) {
        this.timeout = timeout;
    }

    public boolean isActive() {
        return Duration.between(start, Instant.now()).compareTo(timeout) < 0;
    }
}