package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyWriteOperation<K, V, I> implements WriteOperation<K, V, I> {

    private Function<I, K> keyFunction;

    public void setKeyFunction(Function<I, K> function) {
        this.keyFunction = function;
    }

    public void setKey(K key) {
        this.keyFunction = t -> key;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RedisFuture<Object> execute(BaseRedisAsyncCommands<K, V> commands, I item) {
        K key = keyFunction.apply(item);
        if (key == null) {
            return null;
        }
        return (RedisFuture) execute(commands, item, key);
    }

    protected abstract RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, I item, K key);

}
