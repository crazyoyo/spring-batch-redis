package org.springframework.batch.item.redis.support;

import lombok.Builder;

import java.util.Arrays;

public class KeyDumpItemComparator<K, V> extends AbstractItemComparator<K, V, byte[], KeyValue<K, byte[]>> {

    @Builder
    public KeyDumpItemComparator(KeyDumpItemReader<K, V, ?> targetReader, long ttlTolerance) {
        super(targetReader, ttlTolerance);
    }

    @Override
    protected boolean equals(byte[] source, byte[] target) {
        return Arrays.equals(source, target);
    }


}
