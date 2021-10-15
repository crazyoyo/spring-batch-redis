package com.redis.spring.batch.support;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.springframework.util.Assert;

/**
 * Helper class for spring-batch-redis metrics.
 * 
 * @author Julien Ruaux
 */
public interface Utils {

	String METRICS_PREFIX = "spring.batch.redis.";

	static <T extends Collection<?>> T createGaugeCollectionSize(String name, T collection, Tag... tags) {
		return Metrics.globalRegistry.gaugeCollectionSize(METRICS_PREFIX + name, Arrays.asList(tags), collection);
	}

	static void assertPositive(Duration duration, String name) {
		Assert.notNull(duration, name + " must not be null");
		Assert.isTrue(!duration.isZero(), name + " must not be zero");
		Assert.isTrue(!duration.isNegative(), name + " must not be negative");
	}

	static void assertPositive(int value, String name) {
		Assert.isTrue(value > 0, name + " must be greater than zero");
	}

}
