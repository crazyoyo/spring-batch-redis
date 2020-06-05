package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.springframework.batch.item.redis.support.AbstractKeyItemReader;

public class RedisKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyItemReader(StatefulRedisConnection<K, V> connection, Long scanCount, String scanPattern) {
        super(connection, StatefulRedisConnection::sync, scanCount, scanPattern);
    }

    @Builder
    private static RedisKeyItemReader<String, String> build(RedisClient redisClient, Long scanCount, String scanPattern) {
        return new RedisKeyItemReader<>(redisClient.connect(), scanCount, scanPattern);
    }
}
