package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class RedisKeyItemReaderBuilder<K, V> extends AbstractKeyItemReaderBuilder<K, V, RedisKeyItemReaderBuilder<K, V>> {

    private StatefulRedisConnection<K, V> connection;

    public RedisKeyItemReader<K, V> build() {
        Assert.notNull(connection, "A Redis connection is required.");
        return new RedisKeyItemReader<>(connection, commandTimeout, scanCount, scanMatch, sampleSize);
    }

}