package com.redis.spring.batch.writer.operation;

import java.util.List;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class Exec<K, V, T> implements Operation<K, V, T> {

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).exec());
    }

}
