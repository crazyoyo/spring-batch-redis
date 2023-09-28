package com.redis.spring.batch.writer.operation;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class Exec<K, V, T> implements Operation<K, V, T, TransactionResult> {

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<TransactionResult> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return ((RedisTransactionalAsyncCommands<K, V>) commands).exec();
    }

}
