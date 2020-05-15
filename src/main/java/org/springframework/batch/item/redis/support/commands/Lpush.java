package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import org.springframework.batch.item.redis.support.WriteCommand;

public class Lpush<K, V> implements WriteCommand<K, V, MemberArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, MemberArgs<K, V> args) {
        return ((RedisListAsyncCommands<K, V>) commands).lpush(args.getKey(), args.getMemberId());
    }

}