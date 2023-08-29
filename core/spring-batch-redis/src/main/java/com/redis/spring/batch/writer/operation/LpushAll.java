package com.redis.spring.batch.writer.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class LpushAll<K, V, T> extends AbstractPushAllOperation<K, V, T, LpushAll<K, V, T>> {

    @Override
    protected RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values) {
        return commands.lpush(key, values);
    }

}
