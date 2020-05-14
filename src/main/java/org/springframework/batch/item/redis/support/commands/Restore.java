package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V> extends AbstractRestore<K, V> {

    @Override
    protected RedisFuture<?> restore(RedisKeyAsyncCommands<K, V> commands, K key, long ttl, byte[] value) {
        return commands.restore(key, ttl, value);
    }

}