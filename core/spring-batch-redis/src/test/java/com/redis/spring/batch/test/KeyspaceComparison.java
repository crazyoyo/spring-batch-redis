package com.redis.spring.batch.test;

import java.util.List;
import java.util.stream.Collectors;

import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyspaceComparison {

	private final List<KeyComparison> keyComparisons;

	public KeyspaceComparison(List<KeyComparison> comparisons) {
		this.keyComparisons = comparisons;
	}

	public boolean isOk() {
		return mismatches().isEmpty();
	}

	public List<KeyComparison> mismatches() {
		return keyComparisons.stream().filter(c -> c.getStatus() != Status.OK).collect(Collectors.toList());
	}

	public List<KeyComparison> get(Status status) {
		return keyComparisons.stream().filter(c -> c.getStatus() == status).collect(Collectors.toList());
	}

}
