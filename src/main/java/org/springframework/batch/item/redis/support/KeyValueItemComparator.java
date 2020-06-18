package org.springframework.batch.item.redis.support;

public class KeyValueItemComparator<K, V> extends AbstractRedisItemComparator<K, V, Object, KeyValue<K>> {

    public KeyValueItemComparator(KeyValueItemProcessor<K, V> targetProcessor, long ttlTolerance) {
        super(targetProcessor, ttlTolerance);
    }

    @Override
    protected boolean equals(Object source, Object target) {
        return source.equals(target);
    }
}
