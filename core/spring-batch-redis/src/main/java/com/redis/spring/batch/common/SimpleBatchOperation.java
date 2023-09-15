package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.common.BatchOperationFunction.BatchOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class SimpleBatchOperation<K, V, I, O> implements BatchOperation<K, V, I, O> {

    private final Operation<K, V, I, O> operation;

    public SimpleBatchOperation(Operation<K, V, I, O> operation) {
        this.operation = operation;
    }

    @Override
    public List<RedisFuture<O>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items) {
        List<RedisFuture<O>> futures = new ArrayList<>(items.size());
        for (I item : items) {
            operation.execute(commands, item, futures);
        }
        return futures;
    }

}
