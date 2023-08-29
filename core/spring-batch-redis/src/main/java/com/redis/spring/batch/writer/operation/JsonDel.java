package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> extends AbstractOperation<K, V, T, JsonDel<K, V, T>> {

    private Function<T, String> path = JsonSet.rootPath();

    public JsonDel<K, V, T> path(Function<T, String> path) {
        this.path = path;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key(item), path.apply(item)));
    }

}
