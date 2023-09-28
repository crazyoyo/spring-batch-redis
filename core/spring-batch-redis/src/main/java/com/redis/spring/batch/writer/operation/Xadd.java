package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, XAddArgs> argsFunction = t -> null;

    private Function<T, Map<K, V>> bodyFunction;

    public void setArgs(XAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, XAddArgs> function) {
        this.argsFunction = function;
    }

    public void setBodyFunction(Function<T, Map<K, V>> function) {
        this.bodyFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Map<K, V> map = bodyFunction.apply(item);
        if (CollectionUtils.isEmpty(map)) {
            return null;
        }
        XAddArgs args = argsFunction.apply(item);
        return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args, map);
    }

}
