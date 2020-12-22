package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;

import java.time.Duration;

public class RedisKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyItemReader(StatefulRedisConnection<K, V> connection, Duration commandTimeout, long scanCount, String scanMatch) {
        super(connection, commandTimeout, scanCount, scanMatch);
    }

    @Override
    protected RedisAsyncCommands<K, V> async(StatefulRedisConnection<K, V> connection) {
        return connection.async();
    }

    @Override
    protected BaseRedisCommands<K, V> sync(StatefulRedisConnection<K, V> connection) {
        return connection.sync();
    }

}
