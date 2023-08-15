package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> implements Operation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final Function<T, Map<K, V>> map;

    public Hset(Function<T, K> key, Function<T, Map<K, V>> map) {
        this.keyFunction = key;
        Assert.notNull(map, "A map function is required");
        this.map = map;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Map<K, V> value = map.apply(item);
        if (value != null && !value.isEmpty()) {
            K key = keyFunction.apply(item);
            futures.add((RedisFuture) ((RedisHashAsyncCommands<K, V>) commands).hset(key, value));
        }
    }

}
