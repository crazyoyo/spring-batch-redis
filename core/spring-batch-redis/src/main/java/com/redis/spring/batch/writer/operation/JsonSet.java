package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractSingleOperation<K, V, T> {

    public static final String ROOT_PATH = "$";

    private Function<T, String> pathFunction = t -> ROOT_PATH;

    private Function<T, V> valueFunction;

    public void setPath(String path) {
        this.pathFunction = t -> path;
    }

    public void setPathFunction(Function<T, String> path) {
        this.pathFunction = path;
    }

    public void setValueFunction(Function<T, V> value) {
        this.valueFunction = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        String path = pathFunction.apply(item);
        V value = valueFunction.apply(item);
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path, value);
    }

}
