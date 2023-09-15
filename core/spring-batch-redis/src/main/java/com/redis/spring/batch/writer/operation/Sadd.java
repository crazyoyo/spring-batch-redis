package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, V> value;

    public void setValue(Function<T, V> value) {
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key(item), value.apply(item));
    }

}
