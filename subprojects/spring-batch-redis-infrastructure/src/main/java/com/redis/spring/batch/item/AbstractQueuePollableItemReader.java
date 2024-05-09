package com.redis.spring.batch.item;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AbstractQueuePollableItemReader<T> extends AbstractPollableItemReader<T> {

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private BlockingQueue<T> queue;

	@Override
	protected synchronized void doOpen() throws Exception {
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueCapacity);
		}
	}

	@Override
	protected synchronized void doClose() throws Exception {
		queue = null;
	}

	protected boolean isQueueEmpty() {
		return queue.isEmpty();
	}

	/**
	 * 
	 * @param count number of items to read at once
	 * @return up to <code>count</code> items from the queue
	 */
	public List<T> read(int count) {
		List<T> items = new ArrayList<>(count);
		if (queue != null) {
			queue.drainTo(items, count);
		}
		return items;
	}

	@Override
	protected T doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	protected void put(T element) throws InterruptedException {
		queue.put(element);
	}

	protected boolean offer(T element) {
		return queue.offer(element);
	}

	public BlockingQueue<T> getQueue() {
		return queue;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}
}
