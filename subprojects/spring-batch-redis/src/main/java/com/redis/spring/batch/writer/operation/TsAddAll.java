package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> extends AbstractAddAllOperation<K, V, T, Sample> {

	private final AddOptions<K, V> options;

	public TsAddAll(Function<T, K> key, Function<T, Collection<Sample>> samples, AddOptions<K, V> options) {
		super(key, samples);
		this.options = options;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key,
			Collection<Sample> values) {
		for (Sample sample : values) {
			futures.add(((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample, options));
		}
	}

}