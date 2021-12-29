package com.redis.spring.batch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.PollableItemReader;

public class DelegatingPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements PollableItemReader<T> {

	private final ItemReader<T> delegate;
	private Supplier<Exception> exceptionSupplier = () -> new TimeoutException();
	private long interval = 2;

	public DelegatingPollableItemReader(ItemReader<T> delegate) {
		setName(ClassUtils.getShortName(DelegatingPollableItemReader.class));
		this.delegate = delegate;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	public void setExceptionSupplier(Supplier<Exception> exceptionSupplier) {
		this.exceptionSupplier = exceptionSupplier;
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
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
