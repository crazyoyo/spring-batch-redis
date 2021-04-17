package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemWriter;

import java.util.List;

/**
 * An {@link ItemWriter} decorator with a synchronized
 * {@link SynchronizedItemWriter#write write()} method.
 *
 * @param <T> type of object being written
 */
public class SynchronizedItemWriter<T> implements ItemWriter<T> {

	private final ItemWriter<T> delegate;

	public SynchronizedItemWriter(ItemWriter<T> delegate) {
		this.delegate = delegate;
	}

	/**
	 * This method delegates to the {@code write} method of the {@code delegate}.
	 */
	@Override
	public synchronized void write(List<? extends T> items) throws Exception {
		this.delegate.write(items);
	}

}
