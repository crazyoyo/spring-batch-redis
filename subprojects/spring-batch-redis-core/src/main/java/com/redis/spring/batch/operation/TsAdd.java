package com.redis.spring.batch.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Sample> sampleFunction;
	private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

	public TsAdd(Function<T, K> keyFunction, Function<T, Sample> sampleFunction) {
		super(keyFunction);
		this.sampleFunction = sampleFunction;
	}

	public void setOptions(AddOptions<K, V> options) {
		this.optionsFunction = t -> options;
	}

	public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
		this.optionsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		AddOptions<K, V> options = optionsFunction.apply(item);
		Sample sample = sampleFunction.apply(item);
		if (sample != null) {
			outputs.add((RedisFuture) ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample, options));
		}
	}

}
