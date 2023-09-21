package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class Set<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private static final SetArgs DEFAULT_ARGS = new SetArgs();

    private Function<T, V> valueFunction;

    private Function<T, SetArgs> argsFunction = t -> DEFAULT_ARGS;

    public void setValueFunction(Function<T, V> function) {
        this.valueFunction = function;
    }

    public void setArgs(SetArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, SetArgs> function) {
        this.argsFunction = function;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        V value = valueFunction.apply(item);
        SetArgs args = argsFunction.apply(item);
        return ((RedisStringAsyncCommands<K, V>) commands).set(key, value, args);
    }

}
