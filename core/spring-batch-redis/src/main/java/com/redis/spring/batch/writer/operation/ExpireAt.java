package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> implements Operation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final ToLongFunction<T> epochFunction;

    public ExpireAt(Function<T, K> key, ToLongFunction<T> epoch) {
        this.keyFunction = key;
        Assert.notNull(epoch, "A Epoch function is required");
        this.epochFunction = epoch;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        long epoch = epochFunction.applyAsLong(item);
        if (epoch > 0) {
            K key = keyFunction.apply(item);
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key, epoch));
        }
    }

}
