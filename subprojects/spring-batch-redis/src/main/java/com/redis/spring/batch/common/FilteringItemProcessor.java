package com.redis.spring.batch.common;

import java.util.function.Predicate;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;

public class FilteringItemProcessor<T> extends ItemStreamSupport implements ItemProcessor<T, T> {

	private final Predicate<T> filter;

	public FilteringItemProcessor(Predicate<T> filter) {
		this.filter = filter;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (filter instanceof ItemStream) {
			((ItemStream) filter).open(executionContext);
		}
	}

	@Override
	public void close() {
		if (filter instanceof ItemStream) {
			((ItemStream) filter).close();
		}
		super.close();
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (filter instanceof ItemStream) {
			((ItemStream) filter).update(executionContext);
		}
		super.update(executionContext);
	}

	@Override
	public T process(T item) throws Exception {
		if (filter.test(item)) {
			return item;
		}
		return null;
	}

}
