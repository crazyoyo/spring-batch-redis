package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;

import java.time.Duration;

public class RedisKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyItemReader(StatefulRedisConnection<K, V> connection, Duration commandTimeout, long scanCount, String scanMatch, int sampleSize) {
        super(connection, commandTimeout, scanCount, scanMatch, sampleSize);
    }

    @Override
    protected RedisAsyncCommands<K, V> async(StatefulRedisConnection<K, V> connection) throws Exception {
        return connection.async();
    }

    @Override
    protected BaseRedisCommands<K, V> sync(StatefulRedisConnection<K, V> connection) throws Exception {
        return connection.sync();
    }

    public static <K, V> RedisKeyItemReaderBuilder<K, V> builder() {
        return new RedisKeyItemReaderBuilder<>();
    }

}
