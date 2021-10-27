package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.api.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class TsAdd<K, V, T> extends AbstractKeyOperation<K, V, T> {

	protected final Converter<T, Sample> sample;

	public TsAdd(Converter<T, K> key, Predicate<T> delete, Converter<T, Sample> sample) {
		super(key, delete);
		Assert.notNull(sample, "A sample converter is required");
		this.sample = sample;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisTimeSeriesAsyncCommands<K, V>) commands).add(key, sample.convert(item));
	}

	public static <T> TsAddSampleBuilder<T> key(String key) {
		return new TsAddSampleBuilder<>(t -> key);
	}

	public static <T> TsAddSampleBuilder<T> key(Converter<T, String> key) {
		return new TsAddSampleBuilder<>(key);
	}

	public static class TsAddSampleBuilder<T> {

		private final Converter<T, String> key;

		public TsAddSampleBuilder(Converter<T, String> key) {
			this.key = key;
		}

		public TsAddBuilder<T> sample(Converter<T, Sample> sample) {
			return new TsAddBuilder<>(key, sample);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class TsAddBuilder<T> extends DelBuilder<T, TsAddBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, Sample> sample;

		public TsAddBuilder(Converter<T, String> key, Converter<T, Sample> sample) {
			super(sample);
			this.key = key;
			this.sample = sample;
		}

		@Override
		public TsAdd<String, String, T> build() {
			return new TsAdd<>(key, del, sample);
		}
	}

}
