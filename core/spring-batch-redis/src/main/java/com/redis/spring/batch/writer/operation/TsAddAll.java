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

    private Function<T, Collection<Sample>> samplesFunction;

    private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

    public void setSamplesFunction(Function<T, Collection<Sample>> samples) {
        this.samplesFunction = samples;
    }

    public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
        this.optionsFunction = function;
    }

    public void setOptions(AddOptions<K, V> options) {
        this.optionsFunction = t -> options;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        RedisTimeSeriesAsyncCommands<K, V> tsCommands = (RedisTimeSeriesAsyncCommands<K, V>) commands;
        for (Sample sample : samplesFunction.apply(item)) {
            futures.add((RedisFuture) tsCommands.tsAdd(key, sample, optionsFunction.apply(item)));
        }
    }

}
