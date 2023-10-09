package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.writer.BatchWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecBatchWriteOperation<K, V, T> implements BatchWriteOperation<K, V, T> {

    private final BatchWriteOperation<K, V, T> delegate;

    public MultiExecBatchWriteOperation(BatchWriteOperation<K, V, T> delegate) {
        this.delegate = delegate;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<T> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        RedisTransactionalAsyncCommands<K, V> transactionalCommands = (RedisTransactionalAsyncCommands<K, V>) commands;
        futures.add((RedisFuture) transactionalCommands.multi());
        futures.addAll(delegate.execute(commands, items));
        futures.add((RedisFuture) transactionalCommands.exec());
        return futures;
    }

}
