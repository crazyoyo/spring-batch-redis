package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;

import java.time.Duration;
import java.util.function.Predicate;

public class RedisKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyItemReader(StatefulRedisConnection<K, V> connection, Duration commandTimeout, long scanCount, String scanMatch, int sampleSize, Predicate<K> keyPatternPredicate) {
        super(connection, commandTimeout, scanCount, scanMatch, sampleSize, keyPatternPredicate);
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
