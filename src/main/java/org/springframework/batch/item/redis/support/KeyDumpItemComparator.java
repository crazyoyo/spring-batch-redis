package org.springframework.batch.item.redis.support;

import lombok.Builder;
import org.springframework.batch.item.redis.KeyDump;

import java.util.Arrays;

public class KeyDumpItemComparator<K, V> extends AbstractRedisItemComparator<K, V, byte[], KeyDump<K>> {

    @Builder
    public KeyDumpItemComparator(KeyDumpItemReader<K, V, ?> targetReader, long ttlTolerance) {
        super(targetReader, ttlTolerance);
    }

    @Override
    protected boolean equals(byte[] source, byte[] target) {
        return Arrays.equals(source, target);
    }


}
