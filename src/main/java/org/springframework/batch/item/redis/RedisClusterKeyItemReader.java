package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.springframework.batch.item.redis.support.AbstractKeyItemReader;

public class RedisClusterKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterKeyItemReader(StatefulRedisClusterConnection<K, V> connection, Long scanCount, String scanPattern) {
        super(connection, StatefulRedisClusterConnection::sync, scanCount, scanPattern);
    }

    @Builder
    private static RedisClusterKeyItemReader<String, String> build(RedisClusterClient redisClusterClient, Long scanCount, String scanPattern) {
        return new RedisClusterKeyItemReader<>(redisClusterClient.connect(), scanCount, scanPattern);
    }
}
