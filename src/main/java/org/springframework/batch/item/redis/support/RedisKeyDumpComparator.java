package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;

public class RedisKeyDumpComparator<K, V> extends KeyDumpComparator<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyDumpComparator(GenericObjectPool<StatefulRedisConnection<K, V>> pool, ItemProcessor<List<? extends K>, List<? extends KeyDump<K>>> keyProcessor, long commandTimeout, long pttlTolerance) {
        super(pool, StatefulRedisConnection::async, keyProcessor, commandTimeout, pttlTolerance);
    }

}
