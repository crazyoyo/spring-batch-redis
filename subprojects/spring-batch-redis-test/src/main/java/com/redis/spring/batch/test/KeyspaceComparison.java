package com.redis.spring.batch.test;

import java.util.List;
import java.util.stream.Collectors;

import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;

public class KeyspaceComparison<K> {

	private final List<KeyComparison<K>> keyComparisons;

	public KeyspaceComparison(KeyComparisonItemReader<K, ?> reader) throws Exception {
		this(AbstractTestBase.readAllAndClose(reader));
	}

	public KeyspaceComparison(List<KeyComparison<K>> comparisons) {
		this.keyComparisons = comparisons;
	}

	public List<KeyComparison<K>> getAll() {
		return keyComparisons;
	}

	public boolean isOk() {
		return !keyComparisons.isEmpty() && mismatches().isEmpty();
	}

	public List<KeyComparison<K>> mismatches() {
		return keyComparisons.stream().filter(c -> c.getStatus() != Status.OK).collect(Collectors.toList());
	}

	public List<KeyComparison<K>> get(Status status) {
		return keyComparisons.stream().filter(c -> c.getStatus() == status).collect(Collectors.toList());
	}

}
