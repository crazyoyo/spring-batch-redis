package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyDumpItemWriter;

public class RedisKeyDumpItemWriter<K, V> extends KeyDumpItemWriter<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyDumpItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout, boolean replace) {
        super(pool, StatefulRedisConnection::async, commandTimeout, replace);
    }

}
