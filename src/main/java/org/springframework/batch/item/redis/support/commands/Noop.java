package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.support.Command;

public class Noop<K, V, T> implements Command<K, V, T> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
        return null;
    }

}
