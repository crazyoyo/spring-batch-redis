package com.redis.spring.batch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.reader.PollableItemReader;

public class ExceptionThrowingPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements PollableItemReader<T> {

	public static final long DEFAULT_INTERVAL = 2;

	private final ItemReader<T> delegate;
	private Supplier<Exception> exceptionSupplier = () -> new TimeoutException("Simulated timeout");
	private long interval = DEFAULT_INTERVAL;

	public ExceptionThrowingPollableItemReader(ItemReader<T> delegate) {
		setName(ClassUtils.getShortName(ExceptionThrowingPollableItemReader.class));
		this.delegate = delegate;
	}

	@Override
	public void setName(String name) {
		if (delegate instanceof ItemStreamSupport) {
			((ItemStreamSupport) delegate).setName(name + "-delegate");
		}
		super.setName(name);
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
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		super.update(executionContext);
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).update(executionContext);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).close();
		}
		super.close();
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
