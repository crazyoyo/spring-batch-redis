package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractOperation<K, V, T, Hset<K, V, T>> {

    private Function<T, Map<K, V>> map;

    public Hset<K, V, T> map(Function<T, Map<K, V>> map) {
        this.map = map;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        Map<K, V> value = map.apply(item);
        if (value != null && !value.isEmpty()) {
            K key = key(item);
            futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(key, value));
        }
    }

}
