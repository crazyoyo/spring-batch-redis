package com.redis.spring.batch.writer.operation;

import java.util.Collections;
import java.util.List;

import com.redis.spring.batch.writer.BatchWriteOperation;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements WriteOperation<K, V, T>, BatchWriteOperation<K, V, T> {

    @Override
    public RedisFuture<Object> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return null;
    }

    @Override
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        return Collections.emptyList();
    }

}
