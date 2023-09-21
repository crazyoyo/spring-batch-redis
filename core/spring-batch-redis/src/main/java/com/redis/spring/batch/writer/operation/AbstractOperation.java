package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractOperation<K, V, T> implements Operation<K, V, T, Object> {

    private Function<T, K> keyFunction;

    public void setKeyFunction(Function<T, K> function) {
        this.keyFunction = function;
    }

    public void setKey(K key) {
        this.keyFunction = t -> key;
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        K key = keyFunction.apply(item);
        if (key == null) {
            return;
        }
        execute(commands, item, key, futures);
    }

    protected abstract void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures);

}
