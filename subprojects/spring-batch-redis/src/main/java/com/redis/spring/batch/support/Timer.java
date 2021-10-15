package com.redis.spring.batch.support;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

public class Timer {

    private final Instant start = Instant.now();
    private final Duration timeout;
	private long sleep;

    public Timer(Duration timeout, long sleep) {
        this.timeout = timeout;
        this.sleep = sleep;
    }

    public boolean isActive() {
        return Duration.between(start, Instant.now()).compareTo(timeout) < 0;
    }

	public <T> void await(BooleanSupplier condition) throws InterruptedException {
        while (!condition.getAsBoolean() && isActive()) {
            Thread.sleep(sleep);
        }
	}
}