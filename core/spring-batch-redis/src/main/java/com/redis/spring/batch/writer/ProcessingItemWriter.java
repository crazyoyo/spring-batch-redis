package com.redis.spring.batch.writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamWriter;

public class ProcessingItemWriter<K, T> implements ItemStreamWriter<K> {

	private final ItemProcessor<Chunk<? extends K>, Chunk<T>> processor;
	private final ItemStreamWriter<T> writer;

	public ProcessingItemWriter(ItemProcessor<Chunk<? extends K>, Chunk<T>> processor, ItemStreamWriter<T> writer) {
		this.processor = processor;
		this.writer = writer;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
		if (processor instanceof ItemStream) {
			((ItemStream) processor).open(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).update(executionContext);
		}
		if (processor instanceof ItemStream) {
			((ItemStream) processor).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).close();
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
	}

	@Override
	public void write(Chunk<? extends K> items) throws Exception {
		Chunk<T> values = processor.process(items);
		if (values != null) {
			writer.write(values);
		}
	}

}
