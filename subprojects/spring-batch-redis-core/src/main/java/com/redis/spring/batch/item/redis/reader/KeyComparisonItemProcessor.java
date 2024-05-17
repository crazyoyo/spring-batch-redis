package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.OperationExecutor;

public class KeyComparisonItemProcessor<K, V, T extends KeyValue<K, Object>>
		implements ItemProcessor<Iterable<? extends T>, List<KeyComparison<K>>>, ItemStream {

	private final OperationExecutor<K, V, K, T> reader;
	private final KeyComparator<K, V> comparator;

	public KeyComparisonItemProcessor(OperationExecutor<K, V, K, T> reader, KeyComparator<K, V> comparator) {
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
	public List<KeyComparison<K>> process(Iterable<? extends T> items) throws Exception {
		List<K> keys = StreamSupport.stream(items.spliterator(), false).map(KeyValue::getKey)
				.collect(Collectors.toList());
		List<T> targetItems = reader.process(keys);
		Iterator<? extends T> sourceIterator = items.iterator();
		Iterator<T> targetIterator = targetItems == null ? Collections.emptyIterator() : targetItems.iterator();
		List<KeyComparison<K>> comparisons = new ArrayList<>();
		while (sourceIterator.hasNext()) {
			KeyValue<K, Object> source = sourceIterator.next();
			KeyValue<K, Object> target = targetIterator.hasNext() ? targetIterator.next() : null;
			comparisons.add(comparator.compare(source, target));
		}
		return comparisons;
	}

}
