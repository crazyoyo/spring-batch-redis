package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractOperation<K, V, T, TsAdd<K, V, T>> {

    private Function<T, Sample> sample;

    private Function<T, AddOptions<K, V>> options = t -> null;

    public TsAdd<K, V, T> sample(Function<T, Sample> sample) {
        this.sample = sample;
        return this;
    }

    public TsAdd<K, V, T> options(AddOptions<K, V> options) {
        return options(t -> options);
    }

    public TsAdd<K, V, T> options(Function<T, AddOptions<K, V>> options) {
        this.options = options;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key(item), sample(item), options(item)));
    }

    private AddOptions<K, V> options(T item) {
        return options.apply(item);
    }

    private Sample sample(T item) {
        return sample.apply(item);
    }

}
