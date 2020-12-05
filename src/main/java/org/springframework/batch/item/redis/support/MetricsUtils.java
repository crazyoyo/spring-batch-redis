package org.springframework.batch.item.redis.support;

import java.util.Arrays;
import java.util.Collection;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

/**
 * Helper class for spring-batch-redis metrics.
 * 
 * @author Julien Ruaux
 */
public interface MetricsUtils {

	String METRICS_PREFIX = "spring.batch.redis.";

	/**
	 * Create a {@link Timer}.
	 * 
	 * @param name        of the timer. Will be prefixed with
	 *                    {@link MetricsUtils#METRICS_PREFIX}.
	 * @param description of the timer
	 * @param tags        of the timer
	 * @return a new timer instance
	 */
	public static Timer createTimer(String name, String description, Tag... tags) {
		return Timer.builder(METRICS_PREFIX + name).description(description).tags(Arrays.asList(tags))
				.register(Metrics.globalRegistry);
	}

	/**
	 * Create a new {@link Timer.Sample}.
	 * 
	 * @return a new timer sample instance
	 */
	public static Timer.Sample createTimerSample() {
		return Timer.start(Metrics.globalRegistry);
	}

	/**
	 * Create a new {@link LongTaskTimer}.
	 * 
	 * @param name        of the long task timer. Will be prefixed with
	 *                    {@link MetricsUtils#METRICS_PREFIX}.
	 * @param description of the long task timer.
	 * @param tags        of the timer
	 * @return a new long task timer instance
	 */
	public static LongTaskTimer createLongTaskTimer(String name, String description, Tag... tags) {
		return LongTaskTimer.builder(METRICS_PREFIX + name).description(description).tags(Arrays.asList(tags))
				.register(Metrics.globalRegistry);
	}

	public static <T extends Collection<?>> T createGaugeCollectionSize(String name, T collection, Tag... tags) {
		return Metrics.globalRegistry.gaugeCollectionSize(METRICS_PREFIX + name, Arrays.asList(tags), collection);
	}

}
