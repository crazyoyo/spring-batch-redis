package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import org.springframework.batch.item.redis.support.Command;

public class Sadd<K, V> implements Command<K, V, MemberArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, MemberArgs<K, V> args) {
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(args.getKey(), args.getMember());
    }

}