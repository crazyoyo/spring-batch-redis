package com.redis.spring.batch.writer.operation;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Duration> ttl = t -> null;

    public void setTtl(Duration duration) {
        setTtl(t -> duration);
    }

    public void setTtl(Function<T, Duration> function) {
        this.ttl = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        Duration duration = ttl(item);
        if (BatchUtils.isPositive(duration)) {
            futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpire(key(item), duration));
        }
    }

    private Duration ttl(T item) {
        return ttl.apply(item);
    }

}
