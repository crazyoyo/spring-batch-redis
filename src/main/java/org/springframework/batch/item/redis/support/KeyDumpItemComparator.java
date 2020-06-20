package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemProcessor;

import java.util.Arrays;
import java.util.List;

public class KeyDumpItemComparator<K> extends AbstractRedisItemComparator<K, byte[], KeyDump<K>> {

    public KeyDumpItemComparator(ItemProcessor<List<? extends K>, List<KeyDump<K>>> targetProcessor, long ttlTolerance) {
        super(targetProcessor, ttlTolerance);
    }

    @Override
    protected boolean equals(byte[] source, byte[] target) {
        return Arrays.equals(source, target);
    }


}
