package com.redis.spring.batch.support.compare;

import java.util.Map;

import com.redis.spring.batch.support.compare.KeyComparison.Status;

public class KeyComparisonResults {

	private final Map<Status, Long> counts;

	public KeyComparisonResults(Map<Status, Long> counts) {
		this.counts = counts;
	}

	public Long get(Status status) {
		return counts.get(status);
	}

	public boolean isOK() {
		for (Status status : KeyComparison.MISMATCHES) {
			if (counts.get(status) > 0) {
				return false;
			}
		}
		return true;
	}
}
