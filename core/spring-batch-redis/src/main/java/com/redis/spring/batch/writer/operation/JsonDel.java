package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, String> path = JsonSet.rootPath();

    public void setPath(Function<T, String> path) {
        this.path = path;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key(item), path.apply(item));
    }

}
