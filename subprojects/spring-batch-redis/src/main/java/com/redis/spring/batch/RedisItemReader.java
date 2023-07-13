package com.redis.spring.batch;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.common.ValueType;
import com.redis.spring.batch.reader.AbstractRedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V> extends AbstractRedisItemReader<K, V> {

	public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

	public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, ValueType valueType) {
		super(client, codec, new ScanKeyItemReader<>(client, codec), valueType);
	}

	@Override
	protected void doOpen() {
		getKeyReader().setScanOptions(options.getScanOptions());
		super.doOpen();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ScanKeyItemReader<K, V> getKeyReader() {
		return (ScanKeyItemReader<K, V>) super.getKeyReader();
	}

	public static Builder<String, String> client(AbstractRedisClient client) {
		return client(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static class BaseBuilder<K, V, B extends BaseBuilder<K, V, B>> {

		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;

		private JobRepository jobRepository;
		private ReaderOptions options = ReaderOptions.builder().build();
		private ItemProcessor<K, K> keyProcessor;

		protected BaseBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		@SuppressWarnings("unchecked")
		public B jobRepository(JobRepository jobRepository) {
			this.jobRepository = jobRepository;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B options(ReaderOptions options) {
			this.options = options;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B keyProcessor(ItemProcessor<K, K> processor) {
			this.keyProcessor = processor;
			return (B) this;
		}

		protected void configure(AbstractRedisItemReader<K, V> reader) {
			reader.setJobRepository(jobRepository);
			reader.setOptions(options);
			reader.setKeyProcessor(keyProcessor);
		}

		protected <B1 extends BaseBuilder<K, V, B1>> B1 toBuilder(B1 builder) {
			builder.jobRepository(jobRepository);
			builder.options(options);
			builder.keyProcessor(keyProcessor);
			return builder;

		}

	}

	public static class Builder<K, V> extends BaseBuilder<K, V, Builder<K, V>> {

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public LiveRedisItemReader.Builder<K, V> live() {
			return toBuilder(new LiveRedisItemReader.Builder<>(client, codec));
		}

		public RedisItemReader<K, V> struct() {
			return build(ValueType.STRUCT);
		}

		public RedisItemReader<K, V> dump() {
			return build(ValueType.DUMP);
		}

		public RedisItemReader<K, V> build(ValueType valueType) {
			RedisItemReader<K, V> reader = new RedisItemReader<>(client, codec, valueType);
			configure(reader);
			return reader;
		}

	}

}
