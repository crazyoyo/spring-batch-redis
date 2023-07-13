package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

public class ListItemProcessor<I, O> implements ItemProcessor<List<I>, List<O>> {

	private final ItemProcessor<I, O> elementProcessor;

	public ListItemProcessor(ItemProcessor<I, O> elementProcessor) {
		this.elementProcessor = elementProcessor;
	}

	@Override
	public List<O> process(List<I> items) throws Exception {
		List<O> results = new ArrayList<>();
		for (I item : items) {
			O processedItem = elementProcessor.process(item);
			if (processedItem != null) {
				results.add(processedItem);
			}
		}
		return results;
	}

}
