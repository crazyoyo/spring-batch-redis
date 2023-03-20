package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class SynchronizedListItemWriter<T> extends AbstractItemStreamItemWriter<T> {

	private List<T> writtenItems = new ArrayList<>();
	private boolean open;

	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		writtenItems.addAll(items);
	}

	public List<? extends T> getWrittenItems() {
		return this.writtenItems;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		this.open = true;
	}

	@Override
	public void close() {
		super.close();
		this.open = false;
	}

	public boolean isOpen() {
		return open;
	}
}