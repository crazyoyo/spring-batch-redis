package com.redis.spring.batch.common;

import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;

public class FilteringItemProcessor<T> extends DelegatingItemStreamSupport implements ItemProcessor<T, T> {

	private final Predicate<T> filter;

	public FilteringItemProcessor(Predicate<T> filter) {
		super(filter);
		this.filter = filter;
	}

	@Override
	public T process(T item) {
		if (filter.test(item)) {
			return item;
		}
		return null;
	}

}
