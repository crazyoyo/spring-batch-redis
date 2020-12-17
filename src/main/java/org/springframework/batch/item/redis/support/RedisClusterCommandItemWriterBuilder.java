package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class RedisClusterCommandItemWriterBuilder<K, V, T> extends AbstractCommandItemWriterBuilder<K, V, T, RedisClusterCommandItemWriterBuilder<K, V, T>> {

    private GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterCommandItemWriter<K, V, T> build() {
        Assert.notNull(pool, "A connection pool is required.");
        return new RedisClusterCommandItemWriter<>(pool, getCommand(), timeout);
    }

}