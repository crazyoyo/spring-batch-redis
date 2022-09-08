package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.api.StatefulConnection;

public class KeyComparisonValueReader extends AbstractValueReader<String, String, KeyComparison<String>> {

	private final DataStructureValueReader<String, String> left;
	private final DataStructureValueReader<String, String> right;
	private final long ttlToleranceMillis;

	public KeyComparisonValueReader(GenericObjectPool<StatefulConnection<String, String>> left,
			GenericObjectPool<StatefulConnection<String, String>> right, Duration ttlTolerance) {
		super(left);
		Assert.notNull(right, "Right connection pool is required");
		this.left = new DataStructureValueReader<>(left);
		this.right = new DataStructureValueReader<>(right);
		this.ttlToleranceMillis = ttlTolerance.toMillis();
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		right.open(executionContext);
		left.open(executionContext);
		super.open(executionContext);
	}

	@Override
	public void close() {
		super.close();
		left.close();
		right.close();
	}

	@Override
	protected List<KeyComparison<String>> read(StatefulConnection<String, String> connection,
			List<? extends String> keys) throws Exception {
		List<KeyComparison<String>> comparisons = new ArrayList<>();
		List<DataStructure<String>> sourceItems = left.process(keys);
		List<DataStructure<String>> targetItems = right.process(keys);
		Assert.isTrue(targetItems != null && targetItems.size() == keys.size(),
				"Missing values in value reader response");
		for (int index = 0; index < keys.size(); index++) {
			DataStructure<String> source = sourceItems.get(index);
			DataStructure<String> target = targetItems.get(index);
			comparisons.add(new KeyComparison<>(source, target, compare(source, target)));
		}
		return comparisons;
	}

	private Status compare(DataStructure<String> source, DataStructure<String> target) {
		if (!Objects.equals(source.getType(), target.getType())) {
			if (target.getType() == Type.NONE) {
				return Status.MISSING;
			}
			return Status.TYPE;
		}
		if (!Objects.deepEquals(source.getValue(), target.getValue())) {
			return Status.VALUE;
		}
		if (!ttlEquals(source.getTtl(), target.getTtl())) {
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
