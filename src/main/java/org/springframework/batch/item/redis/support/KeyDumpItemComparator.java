package org.springframework.batch.item.redis.support;

import java.util.Arrays;

public class KeyDumpItemComparator<K, V> extends AbstractRedisItemComparator<K, V, byte[], KeyDump<K>> {

    public KeyDumpItemComparator(KeyDumpItemProcessor<K, V> targetProcessor, long ttlTolerance) {
        super(targetProcessor, ttlTolerance);
    }

    @Override
    protected boolean equals(byte[] source, byte[] target) {
        return Arrays.equals(source, target);
    }


}
