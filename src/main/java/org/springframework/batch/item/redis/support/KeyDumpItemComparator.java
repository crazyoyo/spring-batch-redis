package org.springframework.batch.item.redis.support;

import java.util.Arrays;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

public class KeyDumpItemComparator<K> extends AbstractRedisItemComparator<K, byte[], KeyValue<K, byte[]>> {

    public KeyDumpItemComparator(ItemProcessor<List<? extends K>, List<KeyValue<K, byte[]>>> targetProcessor,
	    long ttlTolerance) {
	super(targetProcessor, ttlTolerance);
    }

    @Override
    protected boolean equals(byte[] source, byte[] target) {
	return Arrays.equals(source, target);
    }

}
