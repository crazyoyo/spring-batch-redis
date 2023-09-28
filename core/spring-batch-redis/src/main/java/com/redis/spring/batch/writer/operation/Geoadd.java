package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class Geoadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, GeoValue<V>> valueFunction;

    private Function<T, GeoAddArgs> argsFunction = t -> null;

    public void setValueFunction(Function<T, GeoValue<V>> value) {
        this.valueFunction = value;
    }

    public void setArgs(GeoAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, GeoAddArgs> args) {
        this.argsFunction = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        GeoAddArgs args = argsFunction.apply(item);
        GeoValue<V> value = valueFunction.apply(item);
        return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args, value);
    }

}
