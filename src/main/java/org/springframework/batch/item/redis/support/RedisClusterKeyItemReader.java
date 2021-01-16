package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import java.time.Duration;
import java.util.function.Predicate;

public class RedisClusterKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterKeyItemReader(StatefulRedisClusterConnection<K, V> connection, Duration commandTimeout, long scanCount, String scanMatch, int sampleSize, Predicate<K> keyPatternPredicate) {
        super(connection, commandTimeout, scanCount, scanMatch, sampleSize, keyPatternPredicate);
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> async(StatefulRedisClusterConnection<K, V> connection) {
        return connection.async();
    }

    @Override
    protected BaseRedisCommands<K, V> sync(StatefulRedisClusterConnection<K, V> connection) {
        return connection.sync();
    }

}
