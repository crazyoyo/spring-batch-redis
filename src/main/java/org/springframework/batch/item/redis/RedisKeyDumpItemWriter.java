package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyDumpOperation;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.OperationItemWriter;

public class RedisKeyDumpItemWriter<K, V> extends OperationItemWriter<K, V, StatefulRedisConnection<K, V>, KeyValue<K, byte[]>> {

    public RedisKeyDumpItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool) {
        super(pool, StatefulRedisConnection::async, new KeyDumpOperation<>());
    }

    public RedisKeyDumpItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, boolean replace) {
        super(pool, StatefulRedisConnection::async, new KeyDumpOperation<>(replace));
    }

}
