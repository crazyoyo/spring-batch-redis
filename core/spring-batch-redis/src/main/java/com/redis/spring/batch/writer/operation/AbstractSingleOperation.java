package com.redis.spring.batch.writer.operation;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractSingleOperation<K, V, T> extends AbstractOperation<K, V, T> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        futures.add((RedisFuture) execute(commands, item));
    }

    protected abstract RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item);

}
