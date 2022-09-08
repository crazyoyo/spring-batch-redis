package com.redis.spring.batch.reader;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;

public interface ValueReader<K, T> extends ItemProcessor<List<? extends K>, List<T>> {
	
	

}
