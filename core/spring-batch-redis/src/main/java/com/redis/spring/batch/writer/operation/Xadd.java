package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, XAddArgs> args = t -> null;

    private Function<T, Map<K, V>> body;

    public void setArgs(XAddArgs args) {
        setArgs(t -> args);
    }

    public void setArgs(Function<T, XAddArgs> args) {
        this.args = args;
    }

    public void setBody(Function<T, Map<K, V>> body) {
        this.body = body;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Map<K, V> map = body(item);
        if (!map.isEmpty()) {
            futures.add((RedisFuture) ((RedisStreamAsyncCommands<K, V>) commands).xadd(key(item), args(item), map));
        }
    }

    private Map<K, V> body(T item) {
        return body.apply(item);
    }

    private XAddArgs args(T item) {
        return args.apply(item);
    }

}
