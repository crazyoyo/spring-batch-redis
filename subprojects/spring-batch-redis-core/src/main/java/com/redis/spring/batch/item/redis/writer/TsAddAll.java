package com.redis.spring.batch.item.redis.writer;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> keyFunction;
	private final Function<T, Collection<Sample>> samplesFunction;

	private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

	public TsAddAll(Function<T, K> keyFunction, Function<T, Collection<Sample>> samplesFunction) {
		this.keyFunction = keyFunction;
		this.samplesFunction = samplesFunction;
	}

	public TsAddAll<K, V, T> options(AddOptions<K, V> options) {
		return options(t -> options);
	}

	public TsAddAll<K, V, T> options(Function<T, AddOptions<K, V>> function) {
		this.optionsFunction = function;
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> items,
			List<RedisFuture<Object>> futures) {
		RedisTimeSeriesAsyncCommands<K, V> timeseriesCommands = (RedisTimeSeriesAsyncCommands<K, V>) commands;
		for (T item : items) {
			K key = keyFunction.apply(item);
			AddOptions<K, V> options = optionsFunction.apply(item);
			Collection<Sample> samples = samplesFunction.apply(item);
			if (CollectionUtils.isEmpty(samples)) {
				continue;
			}
			for (Sample sample : samples) {
				futures.add((RedisFuture) timeseriesCommands.tsAdd(key, sample, options));
			}
		}
	}

	public static <K, V, T> TsAddAll<K, V, T> of(Function<T, K> key, Function<T, Collection<Sample>> samples,
			AddOptions<K, V> options) {
		TsAddAll<K, V, T> operation = new TsAddAll<>(key, samples);
		operation.options(options);
		return operation;
	}

}
