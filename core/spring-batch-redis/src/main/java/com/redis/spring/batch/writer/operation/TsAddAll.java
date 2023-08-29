package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> extends AbstractOperation<K, V, T, TsAddAll<K, V, T>> {

    private Function<T, Collection<Sample>> samples;

    private AddOptions<K, V> options = null;

    public TsAddAll<K, V, T> samples(Function<T, Collection<Sample>> samples) {
        this.samples = samples;
        return this;
    }

    public TsAddAll<K, V, T> options(AddOptions<K, V> options) {
        this.options = options;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        K key = key(item);
        for (Sample sample : samples(item)) {
            futures.add(((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample, options));
        }
    }

    private Collection<Sample> samples(T item) {
        return samples.apply(item);
    }

}
