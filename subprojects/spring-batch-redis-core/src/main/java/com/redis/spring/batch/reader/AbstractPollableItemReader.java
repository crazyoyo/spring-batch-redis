package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.lang.Nullable;

import com.redis.spring.batch.common.PollableItemReader;

public abstract class AbstractPollableItemReader<T> implements PollableItemReader<T> {

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

	private String name;
	protected Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private int currentItemCount = 0;

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
			item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && !isEnd());
		return item;
	}

	protected boolean isEnd() {
		return false;
	}

	/**
	 * Open resources necessary to start reading input.
	 * 
	 * @throws Exception Allows subclasses to throw checked exceptions for
	 *                   interpretation by the framework
	 */
	protected void doOpen() throws Exception {
		// do nothing
	}

	/**
	 * Close the resources opened in {@link #doOpen()}.
	 * 
	 * @throws Exception Allows subclasses to throw checked exceptions for
	 *                   interpretation by the framework
	 */
	protected void doClose() throws Exception {
		// do nothing
	}

	@Nullable
	@Override
	public T read() throws Exception {
		currentItemCount++;
		return doRead();
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		T item = doPoll(timeout, unit);
		if (item != null) {
			currentItemCount++;
		}
		return item;
	}

	protected abstract T doPoll(long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * Returns the current item count.
	 * 
	 * @return the current item count
	 * @since 5.1
	 */
	public int getCurrentItemCount() {
		return this.currentItemCount;
	}

	@Override
	public void close() throws ItemStreamException {
		try {
			doClose();
		} catch (Exception e) {
			throw new ItemStreamException("Error while closing item reader", e);
		}
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		try {
			doOpen();
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}
		currentItemCount = 0;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

}
