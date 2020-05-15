package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import org.springframework.batch.item.redis.support.WriteCommand;

public class Zadd<K, V> implements WriteCommand<K, V, ZaddArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, ZaddArgs<K, V> args) {
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(args.getKey(), args.getScore(), args.getMemberId());
    }


}