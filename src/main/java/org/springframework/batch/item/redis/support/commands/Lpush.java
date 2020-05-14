package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import lombok.Builder;
import org.springframework.batch.item.redis.support.Command;

public class Lpush<K, V> implements Command<K, V, MemberArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, MemberArgs<K, V> args) {
        return ((RedisListAsyncCommands<K, V>) commands).lpush(args.getKey(), args.getMemberId());
    }

}