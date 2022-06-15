package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;

public class LiveRedisItemReader<K, T extends KeyValue<K, ?>> extends RedisItemReader<K, T>
		implements PollableItemReader<T> {

	private Duration flushingInterval = FlushingSimpleStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	private Optional<Duration> idleTimeout = Optional.empty();

	public LiveRedisItemReader(Builder<K, ?, T> builder) throws Exception {
		super(builder);
	}

	public void setFlushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
	}

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = Optional.of(idleTimeout);
	}

	public void setIdleTimeout(Optional<Duration> idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	@Override
	protected JobExecution execute(Job job) throws JobExecutionException {
		JobExecution execution = super.execute(job);
		jobRunner.awaitRunning(() -> ((AbstractKeyspaceNotificationItemReader<K>) keyReader).isOpen());
		return execution;
	}

	@Override
	protected SimpleStepBuilder<K, K> step() {
		return new FlushingSimpleStepBuilder<>(super.step()).flushingInterval(flushingInterval)
				.idleTimeout(idleTimeout);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return valueQueue.poll(timeout, unit);
	}

	public static class Builder<K, V, T extends KeyValue<K, ?>> extends AbstractBuilder<K, V, T, Builder<K, V, T>> {

		public static final int DEFAULT_DATABASE = 0;
		public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
		private static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);

		private String[] keyPatterns = new String[] { ScanKeyItemReader.DEFAULT_SCAN_MATCH };
		private int database = DEFAULT_DATABASE;
		private int notificationQueueCapacity = AbstractKeyspaceNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
		private Duration flushingInterval = FlushingSimpleStepBuilder.DEFAULT_FLUSHING_INTERVAL;
		private Optional<Duration> idleTimeout = Optional.empty();

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec,
				ValueReaderFactory<K, V, T> valueReaderFactory) {
			super(client, codec, valueReaderFactory);
		}

		public Builder<K, V, T> database(int database) {
			this.database = database;
			return this;
		}

		public Builder<K, V, T> notificationQueueCapacity(int notificationQueueCapacity) {
			this.notificationQueueCapacity = notificationQueueCapacity;
			return this;
		}

		public Builder<K, V, T> flushingInterval(Duration flushingInterval) {
			Utils.assertPositive(flushingInterval, "Flushing interval");
			this.flushingInterval = flushingInterval;
			return this;
		}

		public Builder<K, V, T> idleTimeout(Duration idleTimeout) {
			Utils.assertPositive(idleTimeout, "Idle timeout");
			this.idleTimeout = Optional.of(idleTimeout);
			return this;
		}

		public Builder<K, V, T> idleTimeout(Optional<Duration> idleTimeout) {
			this.idleTimeout = idleTimeout;
			return this;
		}

		public Builder<K, V, T> keyPatterns(String... keyPatterns) {
			this.keyPatterns = keyPatterns;
			return this;
		}

		public LiveRedisItemReader<K, T> build() throws Exception {
			LiveRedisItemReader<K, T> reader = new LiveRedisItemReader<>(this);
			reader.setFlushingInterval(flushingInterval);
			reader.setIdleTimeout(idleTimeout);
			return reader;
		}

		@Override
		public AbstractKeyspaceNotificationItemReader<K> keyReader() {
			AbstractKeyspaceNotificationItemReader<K> reader = liveKeyItemReader();
			reader.setQueueCapacity(notificationQueueCapacity);
			return reader;
		}

		public static List<String> pubSubPatterns(int database, String... keyPatterns) {
			List<String> patterns = new ArrayList<>();
			for (String keyPattern : keyPatterns) {
				patterns.add(pubSubPattern(database, keyPattern));
			}
			return patterns;
		}

		@SuppressWarnings("unchecked")
		private K[] pubSubPatterns() {
			return (K[]) pubSubPatterns(database, keyPatterns).stream().map(this::encodeKey).toArray();
		}

		public static String pubSubPattern(int database, String keyPattern) {
			return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
		}

		private AbstractKeyspaceNotificationItemReader<K> liveKeyItemReader() {
			if (client instanceof RedisClusterClient) {
				return new RedisClusterKeyspaceNotificationItemReader<>(
						() -> ((RedisClusterClient) client).connectPubSub(codec), keyExtractor(), pubSubPatterns());
			}
			return new RedisKeyspaceNotificationItemReader<>(() -> ((RedisClient) client).connectPubSub(codec),
					keyExtractor(), pubSubPatterns());
		}

		private Converter<K, K> keyExtractor() {
			return k -> encodeKey(STRING_KEY_EXTRACTOR.convert(decodeKey(k)));
		}

	}

}
