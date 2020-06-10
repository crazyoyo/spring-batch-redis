package org.springframework.batch.item.redis.support;

import lombok.Builder;
import org.springframework.batch.item.redis.KeyValue;

public class KeyValueItemComparator<K, V> extends AbstractRedisItemComparator<K, V, Object, KeyValue<K>> {

    @Builder
    public KeyValueItemComparator(KeyValueItemReader<K, V, ?> targetReader, long ttlTolerance) {
        super(targetReader, ttlTolerance);
    }

    @Override
    protected boolean equals(Object source, Object target) {
        return source.equals(target);
    }
}
