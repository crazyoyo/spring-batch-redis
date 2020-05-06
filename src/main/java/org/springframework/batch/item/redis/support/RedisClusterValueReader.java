package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;

public class RedisClusterValueReader<K, V> extends AbstractValueReader<K, V> {

    @Getter
    @Setter
    private GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    @Builder
    private RedisClusterValueReader(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, Duration timeout) {
        super(timeout);
        Assert.notNull(pool, "A connection pool is required.");
        this.pool = pool;
    }

    @Override
    public List<KeyValue<K>> process(List<K> keys) throws Exception {
        StatefulRedisClusterConnection<K, V> connection = pool.borrowObject();
        try {
            return read(keys, connection.async());
        } finally {
            pool.returnObject(connection);
        }
    }

}
