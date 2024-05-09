package com.redis.spring.batch.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.item.PollableItemReader;

public class QueueItemReader<T> implements PollableItemReader<T> {

	private final BlockingQueue<T> queue;

	public QueueItemReader(BlockingQueue<T> queue) {
		this.queue = queue;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		throw new UnsupportedOperationException("read not supported");
	}

}