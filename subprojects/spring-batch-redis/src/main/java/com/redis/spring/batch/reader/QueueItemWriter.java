package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.Utils;

public class QueueItemWriter<K, T> extends AbstractItemStreamItemWriter<K> implements Openable {

	private final ReadOperation<K, T> operation;
	private final QueueOptions queueOptions;
	private final BlockingQueue<T> queue;
	private boolean open;

	public QueueItemWriter(ReadOperation<K, T> operation, QueueOptions queueOptions) {
		this.operation = operation;
		this.queueOptions = queueOptions;
		this.queue = new LinkedBlockingQueue<>(queueOptions.getCapacity());
		Utils.createGaugeCollectionSize("reader.queue.size", queue);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (operation instanceof ItemStream) {
			((ItemStream) operation).open(executionContext);
		}
		open = true;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		if (operation instanceof ItemStream) {
			((ItemStream) operation).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (operation instanceof ItemStream) {
			((ItemStream) operation).close();
		}
		super.close();
		open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public void write(List<? extends K> items) throws Exception {
		List<T> values = operation.read(items);
		for (T item : values) {
			try {
				queue.put(item);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			}
		}
	}

	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public int drainTo(List<T> items, int maxElements) {
		return queue.drainTo(items, maxElements);
	}

	public T poll() throws InterruptedException {
		return queue.poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

}