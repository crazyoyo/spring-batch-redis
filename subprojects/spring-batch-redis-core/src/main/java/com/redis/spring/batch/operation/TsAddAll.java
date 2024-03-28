package com.redis.spring.batch.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> implements Operation<K, V, T, Object> {

	private Function<T, K> keyFunction;

	private Function<T, Collection<Sample>> samplesFunction;

	private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

	public void setKey(K key) {
		this.keyFunction = k -> key;
	}

	public void setKeyFunction(Function<T, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	public void setSamplesFunction(Function<T, Collection<Sample>> function) {
		this.samplesFunction = function;
	}

	public void setOptions(AddOptions<K, V> options) {
		this.optionsFunction = t -> options;
	}

	public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
		this.optionsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends T> items,
			Chunk<RedisFuture<Object>> futures) {
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

}
