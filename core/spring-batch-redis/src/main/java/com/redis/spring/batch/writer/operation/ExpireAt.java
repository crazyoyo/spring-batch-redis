package com.redis.spring.batch.writer.operation;

import java.util.function.ToLongFunction;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private ToLongFunction<T> epochFunction;

    public void setEpoch(long epoch) {
        this.epochFunction = t -> epoch;
    }

    public void setEpochFunction(ToLongFunction<T> function) {
        this.epochFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Boolean> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        long millis = epochFunction.applyAsLong(item);
        if (millis > 0) {
            return ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key, millis);
        }
        return null;
    }

}
