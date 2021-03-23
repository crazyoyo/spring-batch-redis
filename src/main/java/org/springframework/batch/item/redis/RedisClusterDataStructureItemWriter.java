package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.DataStructureItemWriter;

public class RedisClusterDataStructureItemWriter<K, V> extends DataStructureItemWriter<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterDataStructureItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool) {
        super(pool, StatefulRedisClusterConnection::async);
    }
}
