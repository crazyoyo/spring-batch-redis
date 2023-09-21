package com.redis.spring.batch.writer.operation;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractSingleOperation<K, V, T> extends AbstractOperation<K, V, T> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        futures.add((RedisFuture) execute(commands, item, key));
    }

    protected abstract RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key);

}
