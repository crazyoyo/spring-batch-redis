package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class TsAdd<K, V, T> extends AbstractOperation<K, V, T> {

    private final Function<T, Sample> sample;

    private final Function<T, AddOptions<K, V>> options;

    public TsAdd(Function<T, K> key, Function<T, Sample> sample) {
        this(key, sample, t -> null);
    }

    public TsAdd(Function<T, K> key, Function<T, Sample> sample, Function<T, AddOptions<K, V>> options) {
        super(key);
        Assert.notNull(sample, "A sample function is required");
        Assert.notNull(options, "Options function is required");
        this.sample = sample;
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(key, sample.apply(item), options.apply(item));
    }

}
