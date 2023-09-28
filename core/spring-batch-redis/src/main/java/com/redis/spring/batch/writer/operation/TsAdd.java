package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

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
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        RedisTimeSeriesAsyncCommands<K, V> timeseriesCommands = (RedisTimeSeriesAsyncCommands<K, V>) commands;
        AddOptions<K, V> options = optionsFunction.apply(item);
        Sample sample = sampleFunction.apply(item);
        if (sample == null) {
            return null;
        }
        return timeseriesCommands.tsAdd(key, sample, options);
    }

}
