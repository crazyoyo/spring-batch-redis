package com.redis.spring.batch.util;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class Await {

	public static final Duration DEFAULT_INITIAL_DELAY = Duration.ZERO;

	public static final Duration DEFAULT_DELAY = Duration.ofMillis(1);

	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(3);

	private Duration initialDelay = DEFAULT_INITIAL_DELAY;

	private Duration delay = DEFAULT_DELAY;

	private Duration timeout = DEFAULT_TIMEOUT;

	public Await delay(Duration delay) {
		this.delay = delay;
		return this;
	}

	public Await initial(Duration initialDelay) {
		this.initialDelay = initialDelay;
		return this;
	}

	/**
	 * Blocks until test is true
	 *
	 * @param test    boolean supplier to wait for
	 * @param timeout the maximum time to wait
	 * @return {@code true} if the test became true within given time, and
	 *         {@code false} if the timeout elapsed before termination
	 * @throws InterruptedException if interrupted while waiting
	 */
	public boolean until(BooleanSupplier test) throws InterruptedException {
		try (ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor()) {
			executor.scheduleWithFixedDelay(() -> {
				if (test.getAsBoolean()) {
					executor.shutdown();
				}
			}, initialDelay.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS);
			return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
		}
	}

	public static Await await() {
		return new Await();
	}

}
