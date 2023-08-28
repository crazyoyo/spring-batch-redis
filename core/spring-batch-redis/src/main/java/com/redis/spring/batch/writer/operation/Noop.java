package com.redis.spring.batch.writer.operation;

import java.util.List;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements Operation<K, V, T> {

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        // do nothing
    }

}
