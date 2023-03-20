package com.redis.spring.batch.common;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public class ProcessingItemWriter<I, O> extends AbstractItemStreamItemWriter<I> implements Openable {

	private final ItemProcessor<List<I>, List<O>> processor;
	private final ItemWriter<O> writer;
	private boolean open;

	public ProcessingItemWriter(ItemProcessor<List<I>, List<O>> processor, ItemWriter<O> writer) {
		setName(ClassUtils.getShortName(getClass()));
		this.processor = processor;
		this.writer = writer;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (processor instanceof ItemStream) {
			((ItemStream) processor).open(executionContext);
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
		this.open = true;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		if (processor instanceof ItemStream) {
			((ItemStream) processor).update(executionContext);
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).update(executionContext);
		}
	}

	@Override
	public void close() {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
		if (processor instanceof ItemStream) {
			((ItemStream) processor).close();
		}
		super.close();
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void write(List<? extends I> items) throws Exception {
		List values = processor.process((List) items);
		if (values == null) {
			return;
		}
		writer.write(values);
	}

}