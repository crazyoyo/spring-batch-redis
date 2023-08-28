package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> implements Operation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final Function<T, XAddArgs> argsFunction;

    private final Function<T, Map<K, V>> bodyFunction;

    public Xadd(Function<T, K> key, Function<T, Map<K, V>> body, Function<T, XAddArgs> args) {
        Assert.notNull(key, "A key function is required");
        Assert.notNull(body, "A bodyFunction function is required");
        Assert.notNull(args, "Args function is required");
        this.keyFunction = key;
        this.bodyFunction = body;
        this.argsFunction = args;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Map<K, V> body = bodyFunction.apply(item);
        if (body.isEmpty()) {
            return;
        }
        K key = keyFunction.apply(item);
        XAddArgs args = argsFunction.apply(item);
        RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
        futures.add((RedisFuture) streamCommands.xadd(key, args, body));
    }

}
