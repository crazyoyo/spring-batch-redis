package com.redis.spring.batch.common;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;

public class ProcessingItemWriter<I, O> extends DelegatingItemStreamSupport implements ItemStreamWriter<I> {

	private final ItemProcessor<List<? extends I>, List<O>> processor;
	private final ItemWriter<O> writer;

	public ProcessingItemWriter(ItemProcessor<List<? extends I>, List<O>> processor, ItemWriter<O> writer) {
		super(processor, writer);
		this.processor = processor;
		this.writer = writer;
	}

	@Override
	public void write(List<? extends I> items) throws Exception {
		List<O> list = processor.process(items);
		if (list != null) {
			writer.write(list);
		}
	}

}
