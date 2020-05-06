package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import org.springframework.batch.item.redis.support.Command;

public class Geoadd<K, V> implements Command<K, V, GeoaddArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, GeoaddArgs<K, V> args) {
        return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(args.getKey(), args.getLongitude(), args.getLatitude(), args.getMember());
    }


}
