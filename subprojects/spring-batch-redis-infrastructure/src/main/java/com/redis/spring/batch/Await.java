package com.redis.spring.batch;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

	public Await initialDelay(Duration delay) {
		this.initialDelay = delay;
		return this;
	}

	public Await timeout(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	/**
	 * Blocks until test is true
	 *
	 * @param test boolean supplier to wait for
	 * @throws InterruptedException if interrupted while waiting
	 * @throws TimeoutException     if condition not fulfilled within timeout
	 *                              duration
	 */
	public void until(BooleanSupplier test) throws TimeoutException, InterruptedException {
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			executor.scheduleWithFixedDelay(() -> {
				if (test.getAsBoolean()) {
					executor.shutdown();
				}
			}, initialDelay.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS);
			boolean terminated = executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
			if (!terminated) {
				throw new TimeoutException(String.format("Condition not fulfilled within %s", timeout));
			}
		} finally {
			executor.shutdown();
		}
	}

	public void untilFalse(BooleanSupplier test) throws TimeoutException, InterruptedException {
		until(() -> !test.getAsBoolean());
	}

	public static Await await() {
		return new Await();
	}

}
