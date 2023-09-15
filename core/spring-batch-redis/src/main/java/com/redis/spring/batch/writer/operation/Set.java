package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class Set<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private static final SetArgs DEFAULT_ARGS = new SetArgs();

    private Function<T, V> value;

    private Function<T, SetArgs> args = t -> DEFAULT_ARGS;

    public void setValue(Function<T, V> value) {
        this.value = value;
    }

    public void setArgs(Function<T, SetArgs> args) {
        this.args = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return ((RedisStringAsyncCommands<K, V>) commands).set(key(item), value(item), args(item));
    }

    private SetArgs args(T item) {
        return args.apply(item);
    }

    private V value(T item) {
        return value.apply(item);
    }

}
