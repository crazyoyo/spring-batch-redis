package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;

public class Noop<T> implements OperationItemWriter.RedisOperation<T> {

    @Override
    public <K, V> RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        return null;
    }

}
