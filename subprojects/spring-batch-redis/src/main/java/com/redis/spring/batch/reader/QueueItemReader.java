package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class QueueItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

	private final BlockingQueue<T> queue;
	private final long defaultPollTimeout;

	private boolean open;

	public QueueItemReader(BlockingQueue<T> queue, Duration defaultPollTimeout) {
		Assert.notNull(queue, "Queue must not be null");
		Assert.notNull(defaultPollTimeout, "Default poll timeout must not be null");
		Assert.isTrue(!defaultPollTimeout.isNegative() && !defaultPollTimeout.isZero(),
				"Default poll timeout must be strictly positive");
		setName(ClassUtils.getShortName(getClass()));
		this.queue = queue;
		this.defaultPollTimeout = defaultPollTimeout.toMillis();
	}

	@Override
	protected T doRead() throws InterruptedException {
		return poll(defaultPollTimeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		if (!open) {
			return null;
		}
		return queue.poll(timeout, unit);
	}

	@Override
	protected void doOpen() {
		this.open = true;
	}

	@Override
	protected void doClose() {
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

}
