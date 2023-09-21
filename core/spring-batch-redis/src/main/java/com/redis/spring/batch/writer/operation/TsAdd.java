package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, Sample> sampleFunction;

    private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

    public void setSampleFunction(Function<T, Sample> function) {
        this.sampleFunction = function;
    }

    public void setOptions(AddOptions<K, V> options) {
        this.optionsFunction = t -> options;
    }

    public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
        this.optionsFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Sample sample = sampleFunction.apply(item);
        AddOptions<K, V> options = optionsFunction.apply(item);
        return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample, options);
    }

}
