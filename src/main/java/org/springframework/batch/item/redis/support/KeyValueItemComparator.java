package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.redis.KeyValue;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;

public class KeyValueItemComparator<K, V> extends AbstractRedisItemComparator<K, V, Object, KeyValue<K>> {

    public KeyValueItemComparator(RedisKeyValueItemReader<K, V> targetReader, long ttlTolerance) {
        super(targetReader, ttlTolerance);
    }

    @Override
    protected boolean equals(Object source, Object target) {
        return source.equals(target);
    }
}
