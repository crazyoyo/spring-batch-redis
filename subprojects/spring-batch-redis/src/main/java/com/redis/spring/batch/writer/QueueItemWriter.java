package com.redis.spring.batch.writer;

import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class QueueItemWriter<T> extends AbstractItemStreamItemWriter<T> {

	private final BlockingQueue<T> queue;

	public QueueItemWriter(BlockingQueue<T> queue) {
		this.queue = queue;
	}

	@Override
	public void write(Chunk<? extends T> chunk) throws Exception {
		for (T element : chunk) {
			queue.put(element);
		}
	}

}
