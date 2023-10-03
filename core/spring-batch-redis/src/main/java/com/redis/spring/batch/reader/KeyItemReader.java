package com.redis.spring.batch.reader;

import org.springframework.batch.item.ItemReader;

public interface KeyItemReader<K> extends ItemReader<K> {

    boolean isOpen();

}
