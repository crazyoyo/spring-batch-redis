package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.TransactionItemWriter;

public class RedisClusterTransactionItemWriter<K, V, T> extends TransactionItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

    public RedisClusterTransactionItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, RedisOperation<K, V, T> operation) {
        super(pool, StatefulRedisClusterConnection::async, operation);
    }
}
