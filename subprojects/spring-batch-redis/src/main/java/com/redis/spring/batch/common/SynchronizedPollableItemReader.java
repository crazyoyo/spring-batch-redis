package com.redis.spring.batch.common;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

public class SynchronizedPollableItemReader<T> extends DelegatingItemStreamSupport
		implements PollableItemReader<T>, InitializingBean {

	private final PollableItemReader<T> delegate;

	public SynchronizedPollableItemReader(PollableItemReader<T> delegate) {
		super(delegate);
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
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.delegate, "A delegate item reader is required");
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		return delegate.poll(timeout, unit);
	}

}
