package com.redis.spring.batch;

import java.time.Duration;

import org.springframework.batch.item.ItemReader;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.KeyComparatorOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonValueReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ValueReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, T> extends AbstractRedisItemReader<K, T> {

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	public RedisItemReader(JobRunner jobRunner, ItemReader<K> keyReader, ValueReader<K, T> valueReader,
			ReaderOptions options) {
		super(jobRunner, keyReader, null, valueReader, options);
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

	public static class Builder<K, V> extends AbstractReaderBuilder<K, V, Builder<K, V>> {

		private ScanOptions scanOptions = ScanOptions.builder().build();

		protected Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public Builder<K, V> scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		@Override
		protected ItemReader<K> keyReader() {
			return new ScanKeyItemReader<>(client, codec, scanOptions);
		}

		public LiveRedisItemReader.Builder<K, V> live() {
			return toBuilder(new LiveRedisItemReader.Builder<>(client, codec)).keyPatterns(scanOptions.getMatch());
		}

		public KeyComparatorBuilder comparator(AbstractRedisClient right) {
			KeyComparatorOptions comparatorOptions = KeyComparatorOptions.builder()
					.leftPoolOptions(readerOptions.getPoolOptions()).rightPoolOptions(readerOptions.getPoolOptions())
					.scanOptions(scanOptions).build();
			return toBuilder(new KeyComparatorBuilder(client, right)).comparatorOptions(comparatorOptions);
		}

		public RedisItemReader<K, DataStructure<K>> dataStructure() {
			return reader(dataStructureValueReader());
		}

		public RedisItemReader<K, KeyDump<K>> keyDump() {
			return reader(keyDumpValueReader());
		}

		private <T> RedisItemReader<K, T> reader(ValueReader<K, T> valueReader) {
			return new RedisItemReader<>(jobRunner(), keyReader(), valueReader, readerOptions);
		}

	}

	public static class KeyComparatorBuilder extends AbstractReaderBuilder<String, String, KeyComparatorBuilder> {

		private KeyComparatorOptions comparatorOptions = KeyComparatorOptions.builder().build();
		private final AbstractRedisClient right;

		protected KeyComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left, StringCodec.UTF8);
			this.right = right;
		}

		public KeyComparatorBuilder comparatorOptions(KeyComparatorOptions options) {
			this.comparatorOptions = options;
			return this;
		}

		public RedisItemReader<String, KeyComparison> build() {
			return new RedisItemReader<>(jobRunner(), keyReader(), valueReader(), readerOptions);
		}

		private KeyComparisonValueReader valueReader() {
			return new KeyComparisonValueReader(client, right, comparatorOptions);
		}

		@Override
		protected ItemReader<String> keyReader() {
			return new ScanKeyItemReader<>(client, StringCodec.UTF8, comparatorOptions.getScanOptions());
		}

	}

}
