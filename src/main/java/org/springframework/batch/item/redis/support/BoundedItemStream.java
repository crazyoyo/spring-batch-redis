package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemStream;

public interface BoundedItemStream extends ItemStream {

	int size();

	int available();

}
