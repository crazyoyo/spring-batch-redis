package com.redis.spring.batch.item;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

public abstract class AbstractPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements PollableItemReader<T> {

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

	protected Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	protected AbstractPollableItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	/**
	 * Read next item from input.
	 * 
	 * @return an item or {@code null} if the data source is exhausted
	 * @throws Exception Allows subclasses to throw checked exceptions for
	 *                   interpretation by the framework
	 */
	@Nullable
	protected T doRead() throws Exception {
		T item;
		do {
			item = doPoll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && !isComplete());
		return item;
	}

	public abstract boolean isComplete();

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		T item = doPoll(timeout, unit);
		if (item != null) {
			setCurrentItemCount(getCurrentItemCount() + 1);
		}
		return item;
	}

	protected abstract T doPoll(long timeout, TimeUnit unit) throws InterruptedException;

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

}
