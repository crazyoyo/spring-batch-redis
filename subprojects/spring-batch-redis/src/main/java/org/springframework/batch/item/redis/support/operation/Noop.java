package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;

public class Noop<K, V, T> implements OperationItemWriter.RedisOperation<K, V, T> {

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return null;
    }

}
