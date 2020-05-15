package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import org.springframework.batch.item.redis.support.WriteCommand;

public class Hmset<K, V> implements WriteCommand<K, V, HmsetArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, HmsetArgs<K, V> args) {
        return ((RedisHashAsyncCommands<K, V>) commands).hmset(args.getKey(), args.getMap());
    }


}