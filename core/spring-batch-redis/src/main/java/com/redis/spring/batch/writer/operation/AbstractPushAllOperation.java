package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAllOperation<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Collection<V>> valuesFunction;

    public void setValuesFunction(Function<T, Collection<V>> function) {
        this.valuesFunction = function;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        Collection<V> values = valuesFunction.apply(item);
        if (!CollectionUtils.isEmpty(values)) {
            RedisListAsyncCommands<K, V> listCommands = (RedisListAsyncCommands<K, V>) commands;
            V[] array = (V[]) values.toArray();
            futures.add((RedisFuture) doPush(listCommands, key, array));
        }
    }

    protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
