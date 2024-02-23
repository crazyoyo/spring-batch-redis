package com.redis.spring.batch.writer.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class RpushAll<K, V, T> extends AbstractPushAll<K, V, T> {

    @Override
    protected RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values) {
        return commands.rpush(key, values);
    }

}
