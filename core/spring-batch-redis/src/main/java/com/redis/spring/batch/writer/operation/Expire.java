package com.redis.spring.batch.writer.operation;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Duration> ttlFunction = t -> Duration.ZERO;

    public void setTtl(Duration duration) {
        this.ttlFunction = t -> duration;
    }

    public void setTtlFunction(Function<T, Duration> function) {
        this.ttlFunction = function;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        Duration duration = ttlFunction.apply(item);
        if (BatchUtils.isPositive(duration)) {
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, duration));
        }
    }

}
