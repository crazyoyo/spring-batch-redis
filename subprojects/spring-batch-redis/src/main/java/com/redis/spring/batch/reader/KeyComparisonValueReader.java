package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.api.StatefulConnection;

public class KeyComparisonValueReader extends ItemStreamSupport
		implements ItemProcessor<List<String>, List<KeyComparison<String>>> {

	private final DataStructureValueReader<String, String> leftValueReader;
	private final DataStructureValueReader<String, String> rightValueReader;
	private final long ttlToleranceMillis;

	public KeyComparisonValueReader(GenericObjectPool<StatefulConnection<String, String>> left,
			GenericObjectPool<StatefulConnection<String, String>> right, Duration ttlTolerance) {
		Assert.notNull(left, "Right connection pool is required");
		Assert.notNull(right, "Right connection pool is required");
		this.leftValueReader = new DataStructureValueReader<>(left);
		this.rightValueReader = new DataStructureValueReader<>(right);
		this.ttlToleranceMillis = ttlTolerance.toMillis();
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		rightValueReader.open(executionContext);
		leftValueReader.open(executionContext);
		super.open(executionContext);
	}

	@Override
	public void close() {
		super.close();
		leftValueReader.close();
		rightValueReader.close();
	}

	@Override
	public List<KeyComparison<String>> process(List<String> keys) throws Exception {
		List<KeyComparison<String>> comparisons = new ArrayList<>();
		List<DataStructure<String>> leftItems;
		try {
			leftItems = leftValueReader.process(keys);
		} catch (Exception e) {
			throw new ExecutionException("Could not read keys from left", e);
		}
		List<DataStructure<String>> rightItems;
		try {
			rightItems = rightValueReader.process(keys);
		} catch (Exception e) {
			throw new ExecutionException("Could not read keys from right", e);
		}
		Assert.isTrue(rightItems != null && rightItems.size() == keys.size(),
				"Missing values in value reader response");
		for (int index = 0; index < keys.size(); index++) {
			DataStructure<String> leftItem = leftItems.get(index);
			DataStructure<String> rightItem = rightItems.get(index);
			comparisons.add(new KeyComparison<>(leftItem, rightItem, compare(leftItem, rightItem)));
		}
		return comparisons;
	}

	private Status compare(DataStructure<String> left, DataStructure<String> right) {
		if (!Objects.equals(left.getType(), right.getType())) {
			if (right.getType() == Type.NONE) {
				return Status.MISSING;
			}
			return Status.TYPE;
		}
		if (!Objects.deepEquals(left.getValue(), right.getValue())) {
			return Status.VALUE;
		}
		if (!ttlEquals(left.getTtl(), right.getTtl())) {
			return Status.TTL;
		}
		return Status.OK;

	}

	private boolean ttlEquals(Long source, Long target) {
		if (source == null) {
			return target == null;
		}
		if (target == null) {
			return false;
		}
		return Math.abs(source - target) <= ttlToleranceMillis;
	}

	public static Builder comparator(GenericObjectPool<StatefulConnection<String, String>> left,
			GenericObjectPool<StatefulConnection<String, String>> right) {
		return new Builder(left, right);
	}

	public static class Builder {

		private static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

		private final GenericObjectPool<StatefulConnection<String, String>> leftConnectionPool;
		private final GenericObjectPool<StatefulConnection<String, String>> rightConnectionPool;
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

		public Builder(GenericObjectPool<StatefulConnection<String, String>> left,
				GenericObjectPool<StatefulConnection<String, String>> right) {
			this.leftConnectionPool = left;
			this.rightConnectionPool = right;
		}

		public Builder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public KeyComparisonValueReader build() {
			return new KeyComparisonValueReader(leftConnectionPool, rightConnectionPool, ttlTolerance);
		}

	}

}
