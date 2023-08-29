package com.redis.spring.batch.writer.operation;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Del<K, V, T> extends AbstractOperation<K, V, T, Del<K, V, T>> {

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(key(item)));
    }

}
