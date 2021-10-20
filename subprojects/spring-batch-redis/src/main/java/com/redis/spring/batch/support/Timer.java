package com.redis.spring.batch.support;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import lombok.Setter;

public class Timer {

	public static final long DEFAULT_SLEEP = 1;

	private final Instant start = Instant.now();
	private final Duration timeout;
	@Setter
	private long sleep = DEFAULT_SLEEP;

	public Timer(Duration timeout) {
		this.timeout = timeout;
	}

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

	public static Timer timeout(Duration timeout) {
		return new Timer(timeout);
	}

	public static Timer ofSeconds(long seconds) {
		return new Timer(Duration.ofSeconds(seconds));
	}

	public static Timer ofMillis(long millis) {
		return new Timer(Duration.ofMillis(millis));
	}
}