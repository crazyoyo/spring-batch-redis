package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.springframework.batch.item.redis.support.Command;

public class Set<K, V> implements Command<K, V, SetArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, SetArgs<K, V> args) {
        return ((RedisStringAsyncCommands<K, V>) commands).set(args.getKey(), args.getValue());
    }


}
