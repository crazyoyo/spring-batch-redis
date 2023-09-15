package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.ToLongFunction;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> extends AbstractOperation<K, V, T> {

    private ToLongFunction<T> epoch;

    public void setEpoch(ToLongFunction<T> function) {
        this.epoch = function;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        long millis = epoch.applyAsLong(item);
        if (millis > 0) {
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key(item), millis));
        }
    }

}
