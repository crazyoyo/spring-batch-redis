package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemReader;
import org.springframework.lang.Nullable;

/**
 * 
 * This is a simple ItemReader decorator with a synchronized ItemReader.read()
 * method - which makes a non-thread-safe ItemReader thread-safe.
 *
 * @param <T> type of object being read
 */
public class SynchronizedItemReader<T> implements ItemReader<T> {

	private final ItemReader<T> delegate;

	public SynchronizedItemReader(ItemReader<T> delegate) {
		this.delegate = delegate;
	}

	/**
	 * This delegates to the read method of the <code>delegate</code>
	 */
	@Nullable
	public synchronized T read() throws Exception {
		return this.delegate.read();
	}

}
