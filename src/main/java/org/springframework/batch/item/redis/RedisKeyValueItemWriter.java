package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyValueItemWriter;

public class RedisKeyValueItemWriter<K, V> extends KeyValueItemWriter<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyValueItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout) {
        super(pool, StatefulRedisConnection::async, commandTimeout);
    }

}
