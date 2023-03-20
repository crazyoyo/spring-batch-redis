package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.FlushingOptions;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class LiveRedisItemReader<K, T> extends AbstractRedisItemReader<K, T> implements PollableItemReader<T> {

	public static final int DEFAULT_DATABASE = 0;
	protected static final String[] DEFAULT_KEY_PATTERNS = { ScanOptions.DEFAULT_MATCH };
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

	private final FlushingOptions flushingOptions;

	public LiveRedisItemReader(JobRunner jobRunner, PollableItemReader<K> keyReader, ItemProcessor<K, K> keyProcessor,
			ValueReader<K, T> valueReader, ReaderOptions options, FlushingOptions flushingOptions) {
		super(jobRunner, keyReader, keyProcessor, valueReader, options);
		this.flushingOptions = flushingOptions;
	}

	public static String[] defaultKeyPatterns() {
		return DEFAULT_KEY_PATTERNS;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		super.doOpen();
		Awaitility.await().timeout(JobRunner.DEFAULT_RUNNING_TIMEOUT).until(this::isOpen);
	}

	@Override
	public boolean isOpen() {
		return super.isOpen() && ((PollableItemReader<K>) keyReader).isOpen();
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	protected SimpleStepBuilder<K, K> step() {
		return JobRunner.flushing(super.step(), flushingOptions);
	}

	public static Builder<String, String> client(RedisModulesClusterClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static Builder<String, String> client(RedisModulesClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static <K, V> Builder<K, V> client(RedisModulesClusterClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static String[] patterns(int database, String... keyPatterns) {
		return Stream.of(keyPatterns).map(p -> String.format(PUBSUB_PATTERN_FORMAT, database, p))
				.collect(Collectors.toList()).toArray(new String[0]);
	}

	public static class Builder<K, V> extends AbstractReaderBuilder<K, V, Builder<K, V>> {

		private FlushingOptions flushingOptions = FlushingOptions.builder().build();
		private int database = DEFAULT_DATABASE;
		private String[] keyPatterns = DEFAULT_KEY_PATTERNS;
		private QueueOptions queueOptions = QueueOptions.builder().build();
		private Predicate<K> keyFilter;

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

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public Builder<K, V> flushingOptions(FlushingOptions options) {
			this.flushingOptions = options;
			return this;
		}

		@Override
		protected KeyspaceNotificationItemReader<K, V> keyReader() {
			return new KeyspaceNotificationItemReader<>(client, codec, queueOptions, patterns(database, keyPatterns));
		}

		public LiveRedisItemReader<K, DataStructure<K>> dataStructure() {
			return reader(dataStructureValueReader());
		}

		public LiveRedisItemReader<K, KeyDump<K>> keyDump() {
			return reader(keyDumpValueReader());
		}

		protected <T> LiveRedisItemReader<K, T> reader(ValueReader<K, T> valueReader) {
			return new LiveRedisItemReader<>(jobRunner(), keyReader(), keyProcessor(), valueReader, readerOptions,
					flushingOptions);
		}

		public Builder<K, V> database(int database) {
			this.database = database;
			return this;
		}

		public Builder<K, V> keyPatterns(String... patterns) {
			this.keyPatterns = patterns;
			return this;
		}

		public Builder<K, V> queueOptions(QueueOptions options) {
			this.queueOptions = options;
			return this;
		}

	}

}
