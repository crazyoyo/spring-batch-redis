package com.redis.spring.batch.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.RedisOperation;

import io.lettuce.core.RedisFuture;

public class Noop<K, V, T> implements RedisOperation<K, V, T> {

    @Override
    public RedisFuture<?> execute(RedisModulesAsyncCommands<K, V> commands, T item) {
        return null;
    }

}
