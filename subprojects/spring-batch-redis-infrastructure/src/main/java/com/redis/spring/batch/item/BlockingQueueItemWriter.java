package com.redis.spring.batch.item;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.util.CollectionUtils;

public class BlockingQueueItemWriter<S, T> implements ItemStreamWriter<S> {

	private final BlockingQueue<T> queue;
	private final ItemProcessor<List<? extends S>, List<T>> processor;

	public BlockingQueueItemWriter(ItemProcessor<List<? extends S>, List<T>> processor, BlockingQueue<T> queue) {
		this.queue = queue;
		this.processor = processor;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).open(executionContext);
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).close();
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		if (processor instanceof ItemStream) {
			((ItemStream) processor).update(executionContext);
		}
	}

	@Override
	public void write(Chunk<? extends S> chunk) throws Exception {
		List<T> processedChunk = processor.process(chunk.getItems());
		if (!CollectionUtils.isEmpty(processedChunk)) {
			for (T element : processedChunk) {
				queue.put(element);
			}
		}
	}

}