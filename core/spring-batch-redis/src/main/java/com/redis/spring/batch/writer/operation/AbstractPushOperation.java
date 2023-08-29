package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushOperation<K, V, T, O extends AbstractPushOperation<K, V, T, O>>
        extends AbstractOperation<K, V, T, O> {

    private Function<T, V> value;

    @SuppressWarnings("unchecked")
    public O value(Function<T, V> function) {
        this.value = function;
        return (O) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(doPush((RedisListAsyncCommands<K, V>) commands, key(item), value(item)));
    }

    private V value(T item) {
        return value.apply(item);
    }

    protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V value);

}
