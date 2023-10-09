package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class SimpleBatchOperation<K, V, I, O> implements BatchOperation<K, V, I, O> {

    private final Operation<K, V, I, O> operation;

    public SimpleBatchOperation(Operation<K, V, I, O> operation) {
        this.operation = operation;
    }

    @Override
    public List<RedisFuture<O>> execute(BaseRedisAsyncCommands<K, V> commands, List<I> items) {
        List<RedisFuture<O>> futures = new ArrayList<>();
        for (I item : items) {
            RedisFuture<O> future = operation.execute(commands, item);
            if (future != null) {
                futures.add(future);
            }
        }
        return futures;
    }

}
