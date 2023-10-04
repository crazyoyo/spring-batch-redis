package com.redis.spring.batch.util;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class Await {

    public static final Duration DEFAULT_INITIAL_DELAY = Duration.ZERO;

    public static final Duration DEFAULT_DELAY = Duration.ofMillis(1);

    private Duration initialDelay = DEFAULT_INITIAL_DELAY;

    private Duration delay = DEFAULT_DELAY;

    public void setDelay(Duration delay) {
        this.delay = delay;
    }

    public void setInitialDelay(Duration initialDelay) {
        this.initialDelay = initialDelay;
    }

    /**
     * Blocks until test is true
     *
     * @param test boolean supplier to wait for
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the test became true within given time, and {@code false} if the timeout elapsed before
     *         termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean await(BooleanSupplier test, Duration timeout) throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(() -> {
            if (test.getAsBoolean()) {
                executor.shutdown();
            }
        }, initialDelay.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS);
        return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

}
