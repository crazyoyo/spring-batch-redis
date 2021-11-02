package com.redis.spring.batch.support;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class RedisValueEnqueuer<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemWriter<K> {

	private final ValueReader<K, T> valueReader;
	private final BlockingQueue<T> queue;

	public RedisValueEnqueuer(ValueReader<K, T> reader, BlockingQueue<T> queue) {
		this.valueReader = reader;
		this.queue = queue;
	}

	@Override
	public void open(ExecutionContext executionContext) {
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
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).close();
		}
		super.close();
	}

	@Override
	public void write(List<? extends K> items) throws Exception {
		List<T> values = valueReader.read(items);
		if (values == null) {
			return;
		}
		for (T value : values) {
			filter(value.getKey());
			queue.put(value);
		}
	}

	public void filter(K key) {
		queue.removeIf(v -> v.getKey().equals(key));
	}

}
