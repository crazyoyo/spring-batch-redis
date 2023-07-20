package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> implements WriteOperation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final ToLongFunction<T> millisFunction;

    public Expire(Function<T, K> key, ToLongFunction<T> millis) {
        this.keyFunction = key;
        Assert.notNull(millis, "A millisFunction function is required");
        this.millisFunction = millis;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        long millis = millisFunction.applyAsLong(item);
        if (millis > 0) {
            RedisKeyAsyncCommands<K, V> keyCommands = (RedisKeyAsyncCommands<K, V>) commands;
            K key = keyFunction.apply(item);
            futures.add((RedisFuture) execute(keyCommands, key, millis));
        }
    }

    protected RedisFuture<Boolean> execute(RedisKeyAsyncCommands<K, V> commands, K key, long millis) {
        return commands.pexpire(key, millis);
    }

}
