package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;

public class RedisClusterKeyDumpComparator<K, V> extends KeyDumpComparator<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyDumpComparator(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ItemProcessor<List<? extends K>, List<? extends KeyDump<K>>> keyProcessor, long commandTimeout, long pttlTolerance) {
        super(pool, StatefulRedisClusterConnection::async, keyProcessor, commandTimeout, pttlTolerance);
    }

}
