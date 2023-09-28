package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, V> valueFunction;

    public void setValueFunction(Function<T, V> function) {
        this.valueFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        V value = valueFunction.apply(item);
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, value);
    }

}
