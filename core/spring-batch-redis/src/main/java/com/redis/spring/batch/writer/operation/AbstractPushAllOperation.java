package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAllOperation<K, V, T, O extends AbstractPushAllOperation<K, V, T, O>>
        extends AbstractOperation<K, V, T, O> {

    private Function<T, Collection<V>> values;

    @SuppressWarnings("unchecked")
    public O values(Function<T, Collection<V>> function) {
        this.values = function;
        return (O) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        Collection<V> collection = values.apply(item);
        if (collection.isEmpty()) {
            return;
        }
        RedisListAsyncCommands<K, V> listCommands = (RedisListAsyncCommands<K, V>) commands;
        futures.add(doPush(listCommands, key(item), (V[]) collection.toArray()));
    }

    protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
