package com.redis.spring.batch;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.PollableItemReader;

import lombok.Builder;

public class DelegatingPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements PollableItemReader<T> {

	private final ItemReader<T> delegate;
	private final Supplier<Exception> exceptionSupplier;
	private final long interval;
	private boolean open;

	@Builder
	public DelegatingPollableItemReader(ItemReader<T> delegate, Supplier<Exception> exceptionSupplier, long interval) {
		setName(ClassUtils.getShortName(DelegatingPollableItemReader.class));
		this.delegate = delegate;
		this.exceptionSupplier = exceptionSupplier;
		this.interval = interval;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws Exception {
		return read();
	}

	@Override
	protected T doRead() throws Exception {
		T result = delegate.read();
		if (getCurrentItemCount() % interval == 0) {
			throw exceptionSupplier.get();
		}
		return result;
	}

	@Override
	protected void doOpen() {
		this.open = true;
		;
	}

	@Override
	protected void doClose() {
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

}
