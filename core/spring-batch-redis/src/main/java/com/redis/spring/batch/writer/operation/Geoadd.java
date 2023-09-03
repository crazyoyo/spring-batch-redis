package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class Geoadd<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, GeoValue<V>> value;

    private Function<T, GeoAddArgs> args = t -> null;

    public void setValue(Function<T, GeoValue<V>> value) {
        this.value = value;
    }

    public void setArgs(Function<T, GeoAddArgs> args) {
        this.args = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(((RedisGeoAsyncCommands<K, V>) commands).geoadd(key(item), args(item), value(item)));
    }

    private GeoValue<V> value(T item) {
        return value.apply(item);
    }

    private GeoAddArgs args(T item) {
        return args.apply(item);
    }

}
