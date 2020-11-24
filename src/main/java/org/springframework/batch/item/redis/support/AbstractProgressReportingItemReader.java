package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

public abstract class AbstractProgressReportingItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements BoundedItemReader<T> {

	private int size;

	protected void setSize(int size) {
		this.size = size;
	}

	@Override
	public void setMaxItemCount(int count) {
		this.size = count;
		super.setMaxItemCount(count);
	}

	@Override
	public int available() {
		return size - getCurrentItemCount();
	}

}
