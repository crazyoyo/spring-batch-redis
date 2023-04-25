package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class ThrottledItemReader<T> extends AbstractItemStreamItemReader<T> implements PollableItemReader<T> {

	private final ItemReader<T> delegate;
	private final long sleep;

	public ThrottledItemReader(ItemReader<T> delegate, Duration sleepDuration) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(delegate, "Reader delegate must not be null");
		Assert.notNull(sleepDuration, "Sleep duration must not be null");
		Assert.isTrue(!sleepDuration.isNegative() && !sleepDuration.isZero(),
				"Sleep duration must be strictly positive");
		this.delegate = delegate;
		this.sleep = sleepDuration.toMillis();
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (delegate instanceof ItemStream) {
			((ItemStream) delegate).close();
		}
		super.close();
	}

	@Override
	public T read() throws Exception {
		sleep();
		return delegate.read();
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		sleep();
		return ((PollableItemReader<T>) delegate).poll(timeout, unit);
	}

	private void sleep() throws InterruptedException {
		Thread.sleep(sleep);
	}

	@Override
	public boolean isOpen() {
		return ((PollableItemReader<T>) delegate).isOpen();
	}
}
