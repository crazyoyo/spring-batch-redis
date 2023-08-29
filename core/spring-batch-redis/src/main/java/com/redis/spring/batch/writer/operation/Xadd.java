package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractOperation<K, V, T, Xadd<K, V, T>> {

    private Function<T, XAddArgs> args;

    private Function<T, Map<K, V>> body;

    public Xadd<K, V, T> args(XAddArgs args) {
        return args(t -> args);
    }

    public Xadd<K, V, T> args(Function<T, XAddArgs> args) {
        this.args = args;
        return this;
    }

    public Xadd<K, V, T> body(Function<T, Map<K, V>> body) {
        this.body = body;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        Map<K, V> map = body(item);
        if (!map.isEmpty()) {
            futures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(key(item), args(item), map));
        }
    }

    private Map<K, V> body(T item) {
        return body.apply(item);
    }

    private XAddArgs args(T item) {
        return args.apply(item);
    }

}
