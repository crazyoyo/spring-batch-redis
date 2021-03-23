package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyDumpItemWriter;

public class RedisClusterKeyDumpItemWriter<K, V> extends KeyDumpItemWriter<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterKeyDumpItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool) {
        super(pool, StatefulRedisClusterConnection::async);
    }

    public RedisClusterKeyDumpItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, boolean replace) {
        super(pool, StatefulRedisClusterConnection::async, replace);
    }

}
