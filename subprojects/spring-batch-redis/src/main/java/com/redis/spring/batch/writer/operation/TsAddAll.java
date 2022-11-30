package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> extends AbstractCollectionAddAll<K, V, T> {

	private final Converter<T, K> key;
	private final Converter<T, Collection<Sample>> samples;
	private final Converter<Sample, AddOptions<K, V>> options;

	public TsAddAll(Predicate<T> delPredicate, Converter<T, K> key, Converter<T, Collection<Sample>> samples,
			Converter<Sample, AddOptions<K, V>> options) {
		super(delPredicate, Del.of(key));
		this.key = key;
		this.samples = samples;
		this.options = options;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Collection<RedisFuture> doExecute(BaseRedisAsyncCommands<K, V> commands, T item) {
		Collection<RedisFuture> futures = new ArrayList<>();
		for (Sample sample : samples.convert(item)) {
			futures.add(((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key.convert(item), sample,
					options.convert(sample)));
		}
		return futures;
	}

	public static <K, T> SamplesBuilder<K, T> key(Converter<T, K> key) {
		return new SamplesBuilder<>(key);
	}

	public static class SamplesBuilder<K, T> {

		private final Converter<T, K> key;

		public SamplesBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> samples(Converter<T, Collection<Sample>> samples) {
			return new Builder<>(key, samples);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Collection<Sample>> samples;
		private Converter<Sample, AddOptions<K, V>> options = s -> null;

		public Builder(Converter<T, K> key, Converter<T, Collection<Sample>> samples) {
			this.key = key;
			this.samples = samples;
		}

		public Builder<K, V, T> options(AddOptions<K, V> options) {
			this.options = s -> options;
			return this;
		}

		public Builder<K, V, T> options(Converter<Sample, AddOptions<K, V>> options) {
			this.options = options;
			return this;
		}

		public TsAddAll<K, V, T> build() {
			return new TsAddAll<>(del, key, samples, options);
		}

	}
}