package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class SaddAll<K, V, T> implements WriteOperation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final Function<T, Collection<V>> valuesFunction;

    public SaddAll(Function<T, K> key, Function<T, Collection<V>> values) {
        this.keyFunction = key;
        this.valuesFunction = values;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Collection<V> values = valuesFunction.apply(item);
        if (values.isEmpty()) {
            return;
        }
        K key = keyFunction.apply(item);
        RedisSetAsyncCommands<K, V> setCommands = (RedisSetAsyncCommands<K, V>) commands;
        futures.add((RedisFuture) setCommands.sadd(key, (V[]) values.toArray()));
    }

}
