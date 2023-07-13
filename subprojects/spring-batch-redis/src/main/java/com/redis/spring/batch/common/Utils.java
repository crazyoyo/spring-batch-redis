package com.redis.spring.batch.common;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyValueReadOperation;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.RedisClusterKeyspaceNotificationPublisher;
import com.redis.spring.batch.reader.RedisKeyspaceNotificationPublisher;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StreamItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisScriptingCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
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

	static Supplier<StatefulConnection<String, String>> connectionSupplier(AbstractRedisClient client) {
		return connectionSupplier(client, Optional.empty());
	}

	static Supplier<StatefulConnection<String, String>> connectionSupplier(AbstractRedisClient client,
			Optional<ReadFrom> readFrom) {
		return connectionSupplier(client, StringCodec.UTF8, readFrom);
	}

	static <K, V> Supplier<StatefulConnection<K, V>> connectionSupplier(AbstractRedisClient client,
			RedisCodec<K, V> codec, Optional<ReadFrom> readFrom) {
		if (client instanceof RedisModulesClusterClient) {
			return () -> {
				StatefulRedisClusterConnection<K, V> connection = ((RedisModulesClusterClient) client).connect(codec);
				readFrom.ifPresent(connection::setReadFrom);
				return connection;
			};
		}
		return () -> ((RedisModulesClient) client).connect(codec);
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

	public static PlatformTransactionManager inMemoryTransactionManager() {
		return new ResourcelessTransactionManager();
	}

	static <K> ItemReader<K> synchronizedReader(ItemReader<K> reader) {
		if (reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<K> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<K>) reader);
			return synchronizedReader;
		}
		return reader;
	}

	@SuppressWarnings("unchecked")
	static String loadScript(AbstractRedisClient client, String filename) {
		byte[] bytes;
		try (InputStream inputStream = KeyValueReadOperation.class.getClassLoader().getResourceAsStream(filename)) {
			bytes = FileCopyUtils.copyToByteArray(inputStream);
		} catch (IOException e) {
			throw new ItemStreamException("Could not read LUA script file " + filename);
		}
		try (StatefulConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			return ((RedisScriptingCommands<String, String>) Utils.sync(connection)).scriptLoad(bytes);
		}
	}

	static long getItemReaderSize(ItemReader<?> reader) {
		if (reader instanceof RedisItemReader) {
			RedisItemReader<?, ?> redisItemReader = (RedisItemReader<?, ?>) reader;
			return scanSizeEstimate(redisItemReader.getClient(), redisItemReader.getOptions().getScanOptions());
		}
		if (reader instanceof KeyComparisonItemReader) {
			return getItemReaderSize(((KeyComparisonItemReader) reader).getLeft());
		}
		if (reader instanceof StreamItemReader) {
			StreamItemReader<?, ?> streamItemReader = (StreamItemReader<?, ?>) reader;
			return streamItemReader.streamLength();
		}
		if (reader instanceof GeneratorItemReader) {
			return ((GeneratorItemReader) reader).size();
		}
		if (reader instanceof ScanKeyItemReader) {
			ScanKeyItemReader<?, ?> scanKeyReader = (ScanKeyItemReader<?, ?>) reader;
			return scanSizeEstimate(scanKeyReader.getClient(), scanKeyReader.getScanOptions());
		}
		return -1;
	}

	static long scanSizeEstimate(AbstractRedisClient client, ScanOptions scanOptions) {
		ScanSizeEstimator estimator = new ScanSizeEstimator(client);
		try {
			return estimator.estimateSize(scanOptions);
		} catch (Exception e) {
			return ScanSizeEstimator.UNKNOWN_SIZE;
		}
	}

	static boolean isOpen(Object object) {
		return isOpen(object, true);
	}

	static boolean isOpen(Object object, boolean defaultValue) {
		Boolean value = isNullableOpen(object);
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

	@SuppressWarnings("rawtypes")
	static Boolean isNullableOpen(Object object) {
		if (object instanceof GeneratorItemReader) {
			return ((GeneratorItemReader) object).isOpen();
		}
		if (object instanceof AbstractRedisItemReader) {
			return ((AbstractRedisItemReader) object).isOpen();
		}
		if (object instanceof ScanKeyItemReader) {
			return ((ScanKeyItemReader) object).isOpen();
		}
		if (object instanceof KeyspaceNotificationItemReader) {
			return ((KeyspaceNotificationItemReader) object).isOpen();
		}
		if (object instanceof StreamItemReader) {
			return ((StreamItemReader) object).isOpen();
		}
		if (object instanceof AbstractOperationItemStreamSupport) {
			return ((AbstractOperationItemStreamSupport) object).isOpen();
		}
		if (object instanceof RedisKeyspaceNotificationPublisher) {
			return ((RedisKeyspaceNotificationPublisher) object).isOpen();
		}
		if (object instanceof RedisClusterKeyspaceNotificationPublisher) {
			return ((RedisClusterKeyspaceNotificationPublisher) object).isOpen();
		}
		if (object instanceof CompositeItemStreamProcessor) {
			return ((CompositeItemStreamProcessor) object).isOpen();
		}
		if (object instanceof KeyComparisonItemReader) {
			return ((KeyComparisonItemReader) object).isOpen();
		}
		return null;
	}

}
