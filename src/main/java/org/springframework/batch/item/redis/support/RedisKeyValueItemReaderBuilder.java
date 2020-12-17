package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

public class RedisKeyValueItemReaderBuilder<K, V, B extends RedisKeyValueItemReaderBuilder<K, V, B>> extends AbstractKeyValueItemReaderBuilder<K, V, B> {

    private GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public B pool(GenericObjectPool<StatefulRedisConnection<K, V>> pool) {
        this.pool = pool;
        return (B) this;
    }

    protected GenericObjectPool<StatefulRedisConnection<K, V>> getPool() {
        Assert.notNull(pool, "A connection pool is required.");
        return pool;
    }
}
