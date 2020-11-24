package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.support.SynchronizedItemStreamReader;

public class FlushableSynchronizedItemStreamReader<T> extends SynchronizedItemStreamReader<T>
		implements FlushableItemStreamReader<T> {

	private FlushableItemStreamReader<T> delegate;

	public void setDelegate(FlushableItemStreamReader<T> delegate) {
		super.setDelegate(delegate);
		this.delegate = delegate;
	}

	@Override
	public void flush() {
		delegate.flush();
	}

}
