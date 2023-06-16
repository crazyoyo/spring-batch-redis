package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.TransactionManager;
import org.springframework.util.Assert;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

/**
 * Helper class for Spring Batch Redis
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

	static void assertPositive(Number value, String name) {
		Assert.notNull(value, name + " must not be null");
		Assert.isTrue(value.doubleValue() > 0, name + " must be greater than zero");
	}

	@SuppressWarnings("unchecked")
	static <K, V, T> T sync(StatefulConnection<K, V> connection) {
		if (connection instanceof StatefulRedisClusterConnection) {
			return (T) ((StatefulRedisClusterConnection<K, V>) connection).sync();
		}
		return (T) ((StatefulRedisConnection<K, V>) connection).sync();
	}

	@SuppressWarnings("unchecked")
	static <K, V, T> T async(StatefulConnection<K, V> connection) {
		if (connection instanceof StatefulRedisClusterConnection) {
			return (T) ((StatefulRedisClusterConnection<K, V>) connection).async();
		}
		return (T) ((StatefulRedisConnection<K, V>) connection).async();
	}

	static <T> List<T> readAll(ItemReader<T> reader)
			throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
		List<T> list = new ArrayList<>();
		T element;
		while ((element = reader.read()) != null) {
			list.add(element);
		}
		return list;
	}

	static <B extends SimpleStepBuilder<?, ?>> B multiThread(B builder, int threads) {
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.setQueueCapacity(threads);
			taskExecutor.afterPropertiesSet();
			builder.taskExecutor(taskExecutor);
			builder.throttleLimit(threads);
		}
		return builder;
	}

	static void setName(Object object, String name) {
		if (object instanceof ItemStreamSupport) {
			((ItemStreamSupport) object).setName(name);
		}
	}

	public static JobRepository inMemoryJobRepository() throws Exception {
		@SuppressWarnings("deprecation")
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		bean.afterPropertiesSet();
		return bean.getObject();
	}

	public static TransactionManager inMemoryTransactionManager() {
		return new ResourcelessTransactionManager();
	}

}
