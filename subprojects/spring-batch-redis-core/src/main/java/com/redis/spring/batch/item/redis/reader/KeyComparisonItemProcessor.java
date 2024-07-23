package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.item.redis.common.KeyEvent;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.OperationExecutor;

public class KeyComparisonItemProcessor<K, V>
		implements ItemProcessor<List<? extends KeyValue<K, Object>>, List<KeyComparison<K>>>, ItemStream {

	private final OperationExecutor<K, V, KeyEvent<K>, KeyValue<K, Object>> reader;
	private final KeyComparator<K> comparator;

	public KeyComparisonItemProcessor(OperationExecutor<K, V, KeyEvent<K>, KeyValue<K, Object>> reader,
			KeyComparator<K> comparator) {
		this.reader = reader;
		this.comparator = comparator;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		reader.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		reader.update(executionContext);
	}

	@Override
	public void close() throws ItemStreamException {
		reader.close();
	}

	@Override
	public List<KeyComparison<K>> process(List<? extends KeyValue<K, Object>> items) throws Exception {
		List<KeyValue<K, Object>> targetItems = reader.process(items);
		Iterator<? extends KeyValue<K, Object>> sourceIterator = items.iterator();
		Iterator<KeyValue<K, Object>> targetIterator = targetItems == null ? Collections.emptyIterator()
				: targetItems.iterator();
		List<KeyComparison<K>> comparisons = new ArrayList<>();
		while (sourceIterator.hasNext()) {
			KeyValue<K, Object> source = sourceIterator.next();
			KeyValue<K, Object> target = targetIterator.hasNext() ? targetIterator.next() : null;
			comparisons.add(comparator.compare(source, target));
		}
		return comparisons;
	}

}
