package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.ToLongFunction;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> extends AbstractOperation<K, V, T> {

    private ToLongFunction<T> epochFunction;

    public void setEpoch(long epoch) {
        this.epochFunction = t -> epoch;
    }

    public void setEpochFunction(ToLongFunction<T> function) {
        this.epochFunction = function;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        long millis = epochFunction.applyAsLong(item);
        if (millis > 0) {
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key, millis));
        }
    }

}
