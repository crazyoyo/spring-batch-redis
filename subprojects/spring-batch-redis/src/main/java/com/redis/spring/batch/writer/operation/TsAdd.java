package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Sample> sample;
	private final Converter<T, AddOptions<K, V>> options;

	public TsAdd(Converter<T, K> key, Predicate<T> delete, Converter<T, Sample> sample,
			Converter<T, AddOptions<K, V>> options) {
		super(key, delete);
		Assert.notNull(sample, "A sample converter is required");
		Assert.notNull(options, "An options converter is required");
		this.sample = sample;
		this.options = options;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample.convert(item), options.convert(item));
	}

	public static <K, V, T> TsAddSampleBuilder<K, V, T> key(K key) {
		return key(t -> key);
	}

	public static <T> TsAddSampleBuilder<String, String, T> key(String key) {
		return key(t -> key);
	}

	public static <K, V, T> TsAddSampleBuilder<K, V, T> key(Converter<T, K> key) {
		return new TsAddSampleBuilder<>(key);
	}

	public static class TsAddSampleBuilder<K, V, T> {

		private final Converter<T, K> key;

		public TsAddSampleBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public TsAddBuilder<K, V, T> sample(Converter<T, Sample> sample) {
			return new TsAddBuilder<>(key, sample);
		}
	}

	public static class TsAddBuilder<K, V, T> extends DelBuilder<K, V, T, TsAddBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Sample> sample;
		private Converter<T, AddOptions<K, V>> options = s -> null;

		public TsAddBuilder(Converter<T, K> key, Converter<T, Sample> sample) {
			super(sample);
			this.key = key;
			this.sample = sample;
		}

		public TsAddBuilder<K, V, T> options(Converter<T, AddOptions<K, V>> options) {
			Assert.notNull(options, "Options must not be null");
			this.options = options;
			return this;
		}

		@Override
		public TsAdd<K, V, T> build() {
			return new TsAdd<>(key, del, sample, options);
		}
	}

}
