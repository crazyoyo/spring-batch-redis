package com.redis.spring.batch.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

public class ErrorItemReader<T> implements PollableItemReader<T> {

	public static final float DEFAULT_ERROR_RATE = .5f;

	private final ItemReader<T> delegate;

	private final Supplier<Exception> exceptionSupplier;

	private float errorRate = DEFAULT_ERROR_RATE;

	private final AtomicLong currentItemCount = new AtomicLong();

	public ErrorItemReader(ItemReader<T> delegate) {
		this(delegate, () -> new TimeoutException("Simulated timeout"));
	}

	public ErrorItemReader(ItemReader<T> delegate, Supplier<Exception> exceptionSupplier) {
		this.delegate = delegate;
		this.exceptionSupplier = exceptionSupplier;
	}

	public void setErrorRate(float rate) {
		Assert.isTrue(rate >= 0 && rate <= 1, "Rate must be between 0 and 1");
		this.errorRate = rate;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) {
		try {
			return read();
		} catch (Exception e) {
			throw new ItemStreamException(e);
		}
	}

	@Override
	public T read() throws UnexpectedInputException, ParseException, NonTransientResourceException, Exception {
		T result = delegate.read();
		if (result != null) {
			if (currentItemCount.getAndIncrement() % Math.round(1 / errorRate) == 0) {
				throw exceptionSupplier.get();
			}
		}
		return result;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).update(executionContext);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).close();
		}
	}

}
