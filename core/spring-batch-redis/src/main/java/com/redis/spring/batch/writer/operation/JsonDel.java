package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, String> pathFunction = t -> JsonSet.ROOT_PATH;

    public void setPath(String path) {
        this.pathFunction = t -> path;
    }

    public void setPathFunction(Function<T, String> path) {
        this.pathFunction = path;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key, pathFunction.apply(item));
    }

}
