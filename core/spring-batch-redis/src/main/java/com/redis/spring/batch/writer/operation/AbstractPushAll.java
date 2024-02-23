package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAll<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, Collection<V>> valuesFunction;

    public void setValuesFunction(Function<T, Collection<V>> function) {
        this.valuesFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Collection<V> values = valuesFunction.apply(item);
        if (CollectionUtils.isEmpty(values)) {
            return null;
        }
        RedisListAsyncCommands<K, V> listCommands = (RedisListAsyncCommands<K, V>) commands;
        V[] array = (V[]) values.toArray();
        return doPush(listCommands, key, array);
    }

    protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
