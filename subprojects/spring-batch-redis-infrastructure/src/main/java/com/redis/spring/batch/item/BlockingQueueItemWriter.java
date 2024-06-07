package com.redis.spring.batch.item;

import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

public class BlockingQueueItemWriter<T> implements ItemWriter<T> {

	private final BlockingQueue<T> queue;

	public BlockingQueueItemWriter(BlockingQueue<T> queue) {
		this.queue = queue;
	}

	@Override
	public void write(Chunk<? extends T> chunk) throws InterruptedException {
		for (T element : chunk) {
			queue.put(element);
		}
	}

}