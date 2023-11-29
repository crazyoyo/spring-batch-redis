package com.redis.spring.batch.writer;

import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamWriter;

public class ProcessingItemWriter<K, T> implements ItemStreamWriter<K> {

	private final ItemProcessor<Iterable<K>, Iterable<T>> processor;

	private final BlockingQueue<T> queue;

	private boolean open;

	public ProcessingItemWriter(ItemProcessor<Iterable<K>, Iterable<T>> processor, BlockingQueue<T> queue) {
		this.processor = processor;
		this.queue = queue;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).open(executionContext);
		}
		open = true;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).close();
		}
		open = false;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void write(Chunk<? extends K> items) throws Exception {
		Iterable<T> values = processor.process((Iterable) items);
		if (values != null) {
			for (T item : values) {
				queue.put(item);
			}
		}
	}

	public boolean isOpen() {
		return open;
	}

	public BlockingQueue<T> getQueue() {
		return queue;
	}

}
