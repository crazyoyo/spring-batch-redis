package com.redis.spring.batch.common;

import com.redis.spring.batch.RedisItemReader;

public class KeyTypeComparisonItemReader extends AbstractKeyComparisonItemReader<KeyType<String>> {

    public KeyTypeComparisonItemReader(RedisItemReader<String, String, KeyType<String>> source,
            RedisItemReader<String, String, KeyType<String>> target) {
        super(source, target);
    }

    @Override
    protected KeyComparator<KeyType<String>> comparator() {
        return new KeyTypeComparator();
    }

}
