package com.redis.spring.batch.common;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.PollingException;

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
		return delegate.read();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(delegate, "A delegate item reader is required");
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException {
		return delegate.poll(timeout, unit);
	}

	public static <T> ItemReader<T> synchronize(ItemReader<T> reader) {
		if (reader instanceof PollableItemReader) {
			return new SynchronizedPollableItemReader<>((PollableItemReader<T>) reader);
		}
		if (reader instanceof ItemStreamReader) {
			return synchronizedItemStreamReader((ItemStreamReader<T>) reader);
		}
		return reader;
	}

	private static <T> ItemReader<T> synchronizedItemStreamReader(ItemStreamReader<T> reader) {
		SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
		synchronizedReader.setDelegate(reader);
		return synchronizedReader;
	}

}
