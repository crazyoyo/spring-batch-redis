package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.OperationItemWriter;
import org.springframework.batch.item.redis.support.RedisOperation;

public class RedisClusterOperationItemWriter<K, V, T> extends OperationItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

    public RedisClusterOperationItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, RedisOperation<K, V, T> operation) {
        super(pool, StatefulRedisClusterConnection::async, operation);
    }
}
