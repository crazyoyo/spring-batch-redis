package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> implements WriteOperation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final Function<T, Collection<Sample>> samplesFunction;

    private final AddOptions<K, V> options;

    public TsAddAll(Function<T, K> key, Function<T, Collection<Sample>> samples, AddOptions<K, V> options) {
        this.keyFunction = key;
        this.samplesFunction = samples;
        this.options = options;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        RedisTimeSeriesAsyncCommands<K, V> tsCommands = (RedisTimeSeriesAsyncCommands<K, V>) commands;
        K key = keyFunction.apply(item);
        for (Sample sample : samplesFunction.apply(item)) {
            futures.add((RedisFuture) tsCommands.tsAdd(key, sample, options));
        }
    }

}
