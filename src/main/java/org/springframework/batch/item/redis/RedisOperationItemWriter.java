package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.OperationItemWriter;
import org.springframework.batch.item.redis.support.RedisOperation;

public class RedisOperationItemWriter<K, V, T> extends OperationItemWriter<K, V, StatefulRedisConnection<K, V>, T> {

    public RedisOperationItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, RedisOperation<K, V, T> operation) {
        super(pool, StatefulRedisConnection::async, operation);
    }
}
