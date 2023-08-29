package com.redis.spring.batch.writer.operation;

import java.util.List;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeOperation<K, V, T> implements Operation<K, V, T> {

    private List<Operation<K, V, T>> delegates;

    public CompositeOperation<K, V, T> delegates(List<Operation<K, V, T>> delegates) {
        this.delegates = delegates;
        return this;
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        for (Operation<K, V, T> delegate : delegates) {
            delegate.execute(commands, item, futures);
        }
    }

}
