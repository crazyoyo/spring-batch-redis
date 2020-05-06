package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.redis.support.Command;

public class Expire<K, V> implements Command<K, V, ExpireArgs<K>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, ExpireArgs<K> args) {
        return ((RedisKeyAsyncCommands<K, V>) commands).expire(args.getKey(), args.getTimeout());
    }

}