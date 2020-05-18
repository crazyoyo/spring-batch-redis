package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisKeyDumpItemProcessor<K, V> extends KeyDumpItemProcessor<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyDumpItemProcessor(GenericObjectPool<StatefulRedisConnection<K, V>> pool, long commandTimeout) {
        super(pool, StatefulRedisConnection::async, commandTimeout);
    }

}
