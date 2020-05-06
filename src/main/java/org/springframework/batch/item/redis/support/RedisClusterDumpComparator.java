package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;

public class RedisClusterDumpComparator<K, V> extends AbstractDumpComparator<K, V> {

    @Getter
    @Setter
    private GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    @Builder
    private RedisClusterDumpComparator(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ItemProcessor<List<K>, List<KeyDump<K>>> reader, Duration timeout, Long pttlTolerance) {
        super(reader, timeout, pttlTolerance);
        Assert.notNull(pool, "A connection pool is required.");
        this.pool = pool;
    }

    @Override
    public List<KeyComparison<K>> process(List<K> keys) throws Exception {
        StatefulRedisClusterConnection<K, V> connection = pool.borrowObject();
        try {
            return compare(keys, connection.async());
        } finally {
            pool.returnObject(connection);
        }
    }

}
