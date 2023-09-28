package com.redis.spring.batch.common;

import com.redis.spring.batch.RedisItemReader;

public class KeyTypeComparisonItemReader extends AbstractKeyComparisonItemReader<KeyValue<String>> {

    public KeyTypeComparisonItemReader(RedisItemReader<String, String, KeyValue<String>> source,
            RedisItemReader<String, String, KeyValue<String>> target) {
        super(source, target);
    }

    @Override
    protected KeyComparator<KeyValue<String>> comparator() {
        return new KeyTypeComparator();
    }

}
