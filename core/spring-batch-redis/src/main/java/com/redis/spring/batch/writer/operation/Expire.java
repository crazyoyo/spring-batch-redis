package com.redis.spring.batch.writer.operation;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> implements Operation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final Function<T, Duration> ttlFunction;

    public Expire(Function<T, K> key, Function<T, Duration> ttl) {
        this.keyFunction = key;
        Assert.notNull(ttl, "A millisFunction function is required");
        this.ttlFunction = ttl;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Duration ttl = ttlFunction.apply(item);
        if (BatchUtils.isPositive(ttl)) {
            K key = keyFunction.apply(item);
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, ttl));
        }
    }

}
