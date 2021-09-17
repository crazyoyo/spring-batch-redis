package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.RedisOperation;

public class Noop<K, V, T> implements RedisOperation<K, V, T> {

    @Override
    public RedisFuture<?> execute(RedisModulesAsyncCommands<K, V> commands, T item) {
        return null;
    }

}
