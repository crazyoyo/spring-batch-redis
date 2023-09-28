package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.writer.BatchWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAddAll<K, V, T> implements BatchWriteOperation<K, V, T> {

    private Function<T, K> keyFunction;

    private Function<T, Collection<Sample>> samplesFunction;

    private Function<T, AddOptions<K, V>> optionsFunction = t -> null;

    public void setKey(K key) {
        this.keyFunction = k -> key;
    }

    public void setKeyFunction(Function<T, K> keyFunction) {
        this.keyFunction = keyFunction;
    }

    public void setSamplesFunction(Function<T, Collection<Sample>> function) {
        this.samplesFunction = function;
    }

    public void setOptions(AddOptions<K, V> options) {
        this.optionsFunction = t -> options;
    }

    public void setOptionsFunction(Function<T, AddOptions<K, V>> function) {
        this.optionsFunction = function;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        if (CollectionUtils.isEmpty(items)) {
            return Collections.emptyList();
        }
        List<RedisFuture<Object>> futures = new ArrayList<>();
        RedisTimeSeriesAsyncCommands<K, V> timeseriesCommands = (RedisTimeSeriesAsyncCommands<K, V>) commands;
        for (T item : items) {
            K key = keyFunction.apply(item);
            AddOptions<K, V> options = optionsFunction.apply(item);
            Collection<Sample> samples = samplesFunction.apply(item);
            if (CollectionUtils.isEmpty(samples)) {
                continue;
            }
            for (Sample sample : samples) {
                futures.add((RedisFuture) timeseriesCommands.tsAdd(key, sample, options));
            }
        }
        return futures;
    }

}
