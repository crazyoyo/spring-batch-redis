package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.StepOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class LiveRedisItemReader<K, T> extends RedisItemReader<K, T> {

	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);
	public static final int DEFAULT_DATABASE = 0;
	protected static final String[] DEFAULT_KEY_PATTERNS = { ScanOptions.DEFAULT_MATCH };
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

	public LiveRedisItemReader(JobRunner jobRunner, PollableItemReader<K> keyReader, ItemProcessor<K, K> keyProcessor,
			QueueItemWriter<K, T> queueWriter, StepOptions stepOptions) {
		super(jobRunner, keyReader, keyProcessor, queueWriter, stepOptions);
	}

	public static String[] defaultKeyPatterns() {
		return DEFAULT_KEY_PATTERNS;
	}

	@Override
	protected synchronized void doOpen() throws JobExecutionException {
		super.doOpen();
		jobRunner.awaitRunning(this::isOpen);
	}

	@Override
	public boolean isOpen() {
		return super.isOpen() && ((PollableItemReader<K>) keyReader).isOpen();
	}

	public static String[] patterns(int database, String... keyPatterns) {
		return Stream.of(keyPatterns).map(p -> String.format(PUBSUB_PATTERN_FORMAT, database, p))
				.collect(Collectors.toList()).toArray(new String[0]);
	}

	public static class Builder<K, V> extends AbstractReaderBuilder<K, V, Builder<K, V>> {

		private int database = DEFAULT_DATABASE;
		private String[] keyPatterns = DEFAULT_KEY_PATTERNS;
		private QueueOptions notificationQueueOptions = QueueOptions.builder().build();
		private Predicate<K> keyFilter;

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
			stepOptions.setFlushingInterval(DEFAULT_FLUSHING_INTERVAL);
		}

		public Builder<K, V> keyFilter(Predicate<K> filter) {
			this.keyFilter = filter;
			return this;
		}

		private ItemProcessor<K, K> keyProcessor() {
			if (keyFilter == null) {
				return null;
			}
			return new FilteringItemProcessor<>(keyFilter);
		}

		@Override
		public LiveRedisItemReader<K, DataStructure<K>> dataStructure() {
			return (LiveRedisItemReader<K, DataStructure<K>>) super.dataStructure();
		}

		@Override
		public LiveRedisItemReader<K, KeyDump<K>> keyDump() {
			return (LiveRedisItemReader<K, KeyDump<K>>) super.keyDump();
		}

		@Override
		protected <T> RedisItemReader<K, T> reader(QueueItemWriter<K, T> queueWriter) {
			return new LiveRedisItemReader<>(jobRunner(), keyReader(), keyProcessor(), queueWriter, stepOptions);
		}

		private KeyspaceNotificationItemReader<K, V> keyReader() {
			return new KeyspaceNotificationItemReader<>(client, codec, notificationQueueOptions,
					patterns(database, keyPatterns));
		}

		public Builder<K, V> database(int database) {
			this.database = database;
			return this;
		}

		public Builder<K, V> keyPatterns(String... patterns) {
			this.keyPatterns = patterns;
			return this;
		}

		public Builder<K, V> notificationQueueOptions(QueueOptions options) {
			this.notificationQueueOptions = options;
			return this;
		}

	}

}
