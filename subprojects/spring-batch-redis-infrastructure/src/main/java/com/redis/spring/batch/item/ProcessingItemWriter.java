package com.redis.spring.batch.item;

import java.util.List;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;

public class ProcessingItemWriter<I, O> implements ItemStreamWriter<I> {

	private final ItemProcessor<Iterable<? extends I>, List<O>> processor;
	private final ItemWriter<O> writer;

	public ProcessingItemWriter(ItemProcessor<Iterable<? extends I>, List<O>> processor, ItemWriter<O> writer) {
		this.processor = processor;
		this.writer = writer;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
		if (processor instanceof ItemStream) {
			((ItemStream) processor).open(executionContext);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).close();
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).update(executionContext);
		}
		if (processor instanceof ItemStream) {
			((ItemStream) processor).update(executionContext);
		}
	}

	@Override
	public void write(Chunk<? extends I> chunk) throws Exception {
		List<O> processedChunk = processor.process(chunk);
		if (processedChunk != null) {
			writer.write(new Chunk<>(processedChunk));
		}

	}

}
