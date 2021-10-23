package com.redis.spring.batch;

import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.testcontainers.RedisModulesContainer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@Testcontainers
public class MetricsTests extends AbstractTestBase {

	@Container
	private static final RedisModulesContainer REDIS = new RedisModulesContainer();

	@Test
	public void testMetrics() throws Exception {
		Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
		SimpleMeterRegistry registry = new SimpleMeterRegistry(new SimpleConfig() {
			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public Duration step() {
				return Duration.ofMillis(1);
			}
		}, Clock.SYSTEM);
		Metrics.addRegistry(registry);
		RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
		Generator.id("metrics").jobFactory(inMemoryJobFactory).client(client).build().call();
		RedisItemReader<String, DataStructure<String>> reader = RedisItemReader.dataStructure(inMemoryJobFactory, client).build();
		reader.open(new ExecutionContext());
		Search search = registry.find("spring.batch.redis.reader.queue.size");
		Assertions.assertNotNull(search.gauge());
		reader.close();
	}
}
