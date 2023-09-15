package com.redis.spring.batch.writer.operation;

import java.util.List;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeOperation<K, V, T> implements Operation<K, V, T, Object> {

    private List<Operation<K, V, T, Object>> delegates;

    public CompositeOperation<K, V, T> delegates(List<Operation<K, V, T, Object>> delegates) {
        this.delegates = delegates;
        return this;
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        for (Operation<K, V, T, Object> delegate : delegates) {
            delegate.execute(commands, item, futures);
        }
    }

}
