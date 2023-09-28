package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Zadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, ScoredValue<V>> valueFunction;

    private Function<T, ZAddArgs> argsFunction = t -> null;

    public void setArgs(ZAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, ZAddArgs> function) {
        this.argsFunction = function;
    }

    public void setValueFunction(Function<T, ScoredValue<V>> function) {
        this.valueFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        ZAddArgs args = argsFunction.apply(item);
        ScoredValue<V> value = valueFunction.apply(item);
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, value);
    }

}
