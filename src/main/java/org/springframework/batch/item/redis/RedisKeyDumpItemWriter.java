package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyDumpItemWriter;

public class RedisKeyDumpItemWriter<K, V> extends KeyDumpItemWriter<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyDumpItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool) {
        super(pool, StatefulRedisConnection::async);
    }

    public RedisKeyDumpItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, boolean replace) {
        super(pool, StatefulRedisConnection::async, replace);
    }

}
