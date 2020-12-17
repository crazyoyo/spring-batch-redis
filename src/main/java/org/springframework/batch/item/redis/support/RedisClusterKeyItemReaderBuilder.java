package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class RedisClusterKeyItemReaderBuilder<K, V> extends AbstractKeyItemReaderBuilder<K, V, RedisClusterKeyItemReaderBuilder<K, V>> {

    private StatefulRedisClusterConnection<K, V> connection;

    public RedisClusterKeyItemReader<K, V> build() {
        Assert.notNull(connection, "A Redis connection is required.");
        return new RedisClusterKeyItemReader<>(connection, commandTimeout, scanCount, scanMatch, sampleSize);
    }

}