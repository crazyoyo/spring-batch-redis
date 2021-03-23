package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.CommandItemWriter;

import java.util.function.BiFunction;

public class RedisCommandItemWriter<K, V, T> extends CommandItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

    public RedisCommandItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command) {
        super(pool, StatefulRedisConnection::async, command);
    }

}
