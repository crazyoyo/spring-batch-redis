package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisKeyValueItemProcessor<K, V> extends KeyValueItemProcessor<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyValueItemProcessor(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout) {
        super(pool, StatefulRedisConnection::async, commandTimeout);
    }

}
