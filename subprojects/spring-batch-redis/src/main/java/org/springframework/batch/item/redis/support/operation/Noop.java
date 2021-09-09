package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.RedisOperation;

public class Noop<K, V, T> implements RedisOperation<K, V, T> {

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return null;
    }

}
