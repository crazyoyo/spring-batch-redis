package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, Sample> sample;
	private final Function<T, AddOptions<K, V>> options;

	public TsAdd(Function<T, K> key, Function<T, Sample> sample) {
		this(key, sample, t -> null);
	}

	public TsAdd(Function<T, K> key, Function<T, Sample> sample, Function<T, AddOptions<K, V>> options) {
		super(key);
		Assert.notNull(sample, "A sample function is required");
		Assert.notNull(options, "Options function is required");
		this.sample = sample;
		this.options = options;
	}

	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(execute(commands, item, key));
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample.apply(item), options.apply(item));
	}

	public static <K, T> SampleBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <T> SampleBuilder<String, T> key(String key) {
		return key(t -> key);
	}

	public static <K, T> SampleBuilder<K, T> key(Function<T, K> key) {
		return new SampleBuilder<>(key);
	}

	public static class SampleBuilder<K, T> {

		private final Function<T, K> key;

		public SampleBuilder(Function<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> sample(Function<T, Sample> sample) {
			return new Builder<>(key, sample);
		}
	}

	public static class Builder<K, V, T> {

		private final Function<T, K> key;
		private final Function<T, Sample> sample;
		private Function<T, AddOptions<K, V>> options = s -> null;

		public Builder(Function<T, K> key, Function<T, Sample> sample) {
			this.key = key;
			this.sample = sample;
		}

		public Builder<K, V, T> options(Function<T, AddOptions<K, V>> options) {
			Assert.notNull(options, "Options must not be null");
			this.options = options;
			return this;
		}

		public TsAdd<K, V, T> build() {
			return new TsAdd<>(key, sample, options);
		}
	}

}
