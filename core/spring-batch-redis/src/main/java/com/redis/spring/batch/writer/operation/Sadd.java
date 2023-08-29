package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractOperation<K, V, T, Sadd<K, V, T>> {

    private Function<T, V> value;

    public Sadd<K, V, T> value(Function<T, V> value) {
        this.value = value;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(key(item), value.apply(item)));
    }

}
