package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.util.Assert;

public class QueueItemReader<T> implements PollableItemReader<T> {

	private final BlockingQueue<T> queue;
	private final long defaultPollTimeout;

	public QueueItemReader(BlockingQueue<T> queue, Duration defaultPollTimeout) {
		Assert.notNull(queue, "Queue must not be null");
		Assert.notNull(defaultPollTimeout, "Default poll timeout must not be null");
		Assert.isTrue(!defaultPollTimeout.isNegative() && !defaultPollTimeout.isZero(),
				"Default poll timeout must be strictly positive");
		this.queue = queue;
		this.defaultPollTimeout = defaultPollTimeout.toMillis();
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return poll(defaultPollTimeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

}
