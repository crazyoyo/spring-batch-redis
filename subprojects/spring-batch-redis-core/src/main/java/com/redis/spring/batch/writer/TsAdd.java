package com.redis.spring.batch.writer;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractKeyValueOperation<K, V, Sample, T> {

	private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

	public TsAdd(Function<T, K> keyFunction, Function<T, Sample> sampleFunction) {
		super(keyFunction, sampleFunction);
	}

	public void setOptions(AddOptions<K, V> options) {
		this.optionsFunction = t -> options;
	}

	public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
		this.optionsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Sample value) {
		AddOptions<K, V> options = optionsFunction.apply(item);
		return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, value, options);
	}

}
