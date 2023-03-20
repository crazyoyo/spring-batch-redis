package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.common.Openable;

public interface PollableItemReader<T> extends ItemReader<T>, Openable {

	/**
	 * Tries to read a piece of input data. If such input is available within the
	 * given duration, advances to the next one otherwise returns <code>null</code>.
	 * 
	 * @param timeout how long to wait before giving up, in units of {@code unit}
	 * @param unit    a {@code TimeUnit} determining how to interpret the
	 *                {@code timeout} parameter
	 * @throws InterruptedException if interrupted while waiting
	 * @return T the item to be processed or {@code null} if the specified waiting
	 *         time elapses before an element is available
	 * @throws PollingException if an exception occurred while polling
	 */
	T poll(long timeout, TimeUnit unit) throws InterruptedException, PollingException;

	public static final class PollingException extends Exception {

		public PollingException(Exception e) {
			super(e);
		}

		private static final long serialVersionUID = 1L;

	}
}
