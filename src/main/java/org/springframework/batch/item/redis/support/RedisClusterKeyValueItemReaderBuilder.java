package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

public class RedisClusterKeyValueItemReaderBuilder<K, V, B extends RedisClusterKeyValueItemReaderBuilder> extends AbstractKeyValueItemReaderBuilder<K, V, RedisClusterKeyValueItemReaderBuilder<K, V, B>> {

    private GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public B pool(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool) {
        this.pool = pool;
        return (B) this;
    }

    protected GenericObjectPool<StatefulRedisClusterConnection<K, V>> getPool() {
        Assert.notNull(pool, "A connection pool is required.");
        return pool;
    }

}
