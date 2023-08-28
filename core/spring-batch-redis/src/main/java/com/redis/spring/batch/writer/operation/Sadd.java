package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractOperation<K, V, T> {

    private final Function<T, V> value;

    public Sadd(Function<T, K> key, Function<T, V> value) {
        super(key);
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, value.apply(item));
    }

}
