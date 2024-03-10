package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.lang.Nullable;

public abstract class AbstractPollableItemReader<T> extends AbstractItemStreamItemReader<T>
		implements PollableItemReader<T> {

	private int currentItemCount = 0;

	/**
	 * Read next item from input.
	 * 
	 * @return an item or {@code null} if the data source is exhausted
	 * @throws Exception Allows subclasses to throw checked exceptions for
	 *                   interpretation by the framework
	 */
	@Nullable
	protected abstract T doRead() throws Exception;

	/**
	 * Open resources necessary to start reading input.
	 * 
	 * @throws Exception Allows subclasses to throw checked exceptions for
	 *                   interpretation by the framework
	 */
	protected abstract void doOpen() throws Exception;

	/**
	 * Close the resources opened in {@link #doOpen()}.
	 * 
	 * @throws Exception Allows subclasses to throw checked exceptions for
	 *                   interpretation by the framework
	 */
	protected abstract void doClose() throws Exception;

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

	@SuppressWarnings("removal")
	@Override
	public void close() throws ItemStreamException {
		super.close();
		try {
			doClose();
		} catch (Exception e) {
			throw new ItemStreamException("Error while closing item reader", e);
		}
	}

	@SuppressWarnings("removal")
	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		try {
			doOpen();
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}
		currentItemCount = 0;

	}

}
