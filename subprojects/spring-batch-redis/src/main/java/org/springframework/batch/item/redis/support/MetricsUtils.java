package org.springframework.batch.item.redis.support;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.Arrays;
import java.util.Collection;

/**
 * Helper class for spring-batch-redis metrics.
 * 
 * @author Julien Ruaux
 */
public interface MetricsUtils {

	String METRICS_PREFIX = "spring.batch.redis.";

	static <T extends Collection<?>> T createGaugeCollectionSize(String name, T collection, Tag... tags) {
		return Metrics.globalRegistry.gaugeCollectionSize(METRICS_PREFIX + name, Arrays.asList(tags), collection);
	}

}
