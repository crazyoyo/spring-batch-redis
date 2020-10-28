package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemProcessor;

import java.util.List;

public class DataStructureItemComparator<K> extends AbstractRedisItemComparator<K, Object, DataStructure<K>> {

	public DataStructureItemComparator(ItemProcessor<List<? extends K>, List<DataStructure<K>>> targetProcessor,
			long ttlTolerance) {
		super(targetProcessor, ttlTolerance);
	}

	@Override
	protected boolean equals(Object source, Object target) {
		return source.equals(target);
	}
}
