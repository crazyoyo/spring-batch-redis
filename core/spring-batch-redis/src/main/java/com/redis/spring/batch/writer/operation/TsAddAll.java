package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Collection<Sample>> samples;

    private AddOptions<K, V> options = null;

    public void setSamples(Function<T, Collection<Sample>> samples) {
        this.samples = samples;
    }

    public void setOptions(AddOptions<K, V> options) {
        this.options = options;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        K key = key(item);
        RedisTimeSeriesAsyncCommands<K, V> tsCommands = (RedisTimeSeriesAsyncCommands<K, V>) commands;
        for (Sample sample : samples(item)) {
            futures.add((RedisFuture) tsCommands.tsAdd(key, sample, options));
        }
    }

    private Collection<Sample> samples(T item) {
        return samples.apply(item);
    }

}
