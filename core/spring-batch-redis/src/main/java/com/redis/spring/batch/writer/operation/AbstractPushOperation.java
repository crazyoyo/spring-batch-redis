package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushOperation<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, V> value;

    public void setValue(Function<T, V> function) {
        this.value = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return doPush((RedisListAsyncCommands<K, V>) commands, key(item), value(item));
    }

    private V value(T item) {
        return value.apply(item);
    }

    protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V value);

}
