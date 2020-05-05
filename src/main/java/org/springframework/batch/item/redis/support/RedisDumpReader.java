package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;

public class RedisDumpReader<K, V> extends AbstractDumpReader<K, V> {

    @Getter
    @Setter
    private GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    @Builder
    public RedisDumpReader(GenericObjectPool<StatefulRedisConnection<K, V>> pool, Duration timeout) {
        super(timeout);
        Assert.notNull(pool, "A connection pool is required.");
        this.pool = pool;
    }

    @Override
    public List<KeyDump<K>> process(List<K> keys) throws Exception {
        StatefulRedisConnection<K, V> connection = pool.borrowObject();
        try {
            return read(keys, connection.async());
        } finally {
            pool.returnObject(connection);
        }
    }

}
