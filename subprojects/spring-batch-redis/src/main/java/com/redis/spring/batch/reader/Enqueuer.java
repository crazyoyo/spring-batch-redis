package com.redis.spring.batch.reader;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;

public class Enqueuer<K, T extends KeyValue<K>> extends AbstractItemStreamItemWriter<K> {

	private final Log log = LogFactory.getLog(getClass());

	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	private final QueueOptions options;
	private final BlockingQueue<T> queue;

	public Enqueuer(ItemProcessor<List<? extends K>, List<T>> valueReader, QueueOptions options) {
		this.valueReader = valueReader;
		this.options = options;
		this.queue = new LinkedBlockingQueue<>(options.getCapacity());
	}

	@Override
	public void open(ExecutionContext executionContext) {
		Utils.createGaugeCollectionSize("reader.queue.size", queue);
		super.open(executionContext);
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (!queue.isEmpty()) {
			log.warn("Closing with items still in queue");
		}
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).close();
		}
		super.close();
	}

	public T poll() throws InterruptedException {
		return queue.poll(options.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public void write(List<? extends K> items) throws Exception {
		List<T> values = valueReader.process(items);
		for (T value : values) {
			queue.removeIf(v -> v.getKey().equals(value.getKey()));
			queue.put(value);
		}
	}

	public void drainTo(List<T> items, int maxElements) {
		queue.drainTo(items, maxElements);
	}

}