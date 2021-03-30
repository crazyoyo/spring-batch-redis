package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.TransactionItemWriter;

public class RedisTransactionItemWriter<K, V, T> extends TransactionItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

    public RedisTransactionItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, RedisOperation<K, V, T> operation) {
        super(pool, StatefulRedisConnection::async, operation);
    }
}
