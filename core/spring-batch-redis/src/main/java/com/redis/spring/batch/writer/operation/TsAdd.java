package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, Sample> sample;

    private Function<T, AddOptions<K, V>> options = t -> null;

    public void setSample(Function<T, Sample> sample) {
        this.sample = sample;
    }

    public void setOptions(AddOptions<K, V> options) {
        setOptions(t -> options);
    }

    public void setOptions(Function<T, AddOptions<K, V>> options) {
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key(item), sample(item), options(item));
    }

    private AddOptions<K, V> options(T item) {
        return options.apply(item);
    }

    private Sample sample(T item) {
        return sample.apply(item);
    }

}
