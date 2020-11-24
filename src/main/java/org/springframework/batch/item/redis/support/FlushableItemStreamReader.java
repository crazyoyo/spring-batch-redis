package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemStreamReader;

public interface FlushableItemStreamReader<T> extends ItemStreamReader<T> {

	void flush();

}
