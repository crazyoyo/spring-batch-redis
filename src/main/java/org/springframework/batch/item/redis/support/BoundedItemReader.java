package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemReader;

public interface BoundedItemReader<T> extends ItemReader<T> {

    int available();

}
