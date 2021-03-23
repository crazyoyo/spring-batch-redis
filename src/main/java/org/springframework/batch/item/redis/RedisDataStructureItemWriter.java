package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.DataStructureItemWriter;

public class RedisDataStructureItemWriter<K, V> extends DataStructureItemWriter<K, V, StatefulRedisConnection<K, V>> {

    public RedisDataStructureItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool) {
        super(pool, StatefulRedisConnection::async);
    }
}
