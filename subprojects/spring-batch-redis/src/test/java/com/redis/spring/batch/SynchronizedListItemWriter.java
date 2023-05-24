package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class SynchronizedListItemWriter<T> extends AbstractItemStreamItemWriter<T> {

	private List<T> items = new ArrayList<>();

	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		this.items.addAll(items);
	}

	public List<? extends T> getItems() {
		return this.items;
	}

}