package com.redis.spring.batch.common;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

public class SynchronizedPollableItemReader<T> implements PollableItemReader<T>, InitializingBean, ItemStream {

	private final PollableItemReader<T> delegate;

	public SynchronizedPollableItemReader(PollableItemReader<T> delegate) {
		this.delegate = delegate;
	}

	/**
	 * This delegates to the read method of the <code>delegate</code>
	 */
	@Nullable
	public synchronized T read() throws Exception {
		return this.delegate.read();
	}

	@Override
	public void close() {
		if (delegate instanceof ItemStream) {
			((ItemStream) this.delegate).close();
		}
	}

	public void open(ExecutionContext executionContext) {
		if (delegate instanceof ItemStream) {
			((ItemStream) this.delegate).open(executionContext);
		}
	}

	public void update(ExecutionContext executionContext) {
		if (delegate instanceof ItemStream) {
			((ItemStream) this.delegate).update(executionContext);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.delegate, "A delegate item reader is required");
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		return delegate.poll(timeout, unit);
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}
}
