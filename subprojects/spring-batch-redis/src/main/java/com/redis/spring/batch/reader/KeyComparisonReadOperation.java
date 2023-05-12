package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonReadOperation extends ItemStreamSupport implements ReadOperation<String, KeyComparison> {

	private final DataStructureReadOperation<String, String> leftReader;
	private final DataStructureReadOperation<String, String> rightReader;
	private final Duration ttlTolerance;

	public KeyComparisonReadOperation(AbstractRedisClient left, AbstractRedisClient right, PoolOptions leftPoolOptions,
			PoolOptions rightPoolOptions, Duration ttlTolerance) {
		this.leftReader = new DataStructureReadOperation<>(left, StringCodec.UTF8, leftPoolOptions);
		this.rightReader = new DataStructureReadOperation<>(right, StringCodec.UTF8, rightPoolOptions);
		this.ttlTolerance = ttlTolerance;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		rightReader.open(executionContext);
		leftReader.open(executionContext);
		super.open(executionContext);
	}

	@Override
	public void close() {
		super.close();
		leftReader.close();
		rightReader.close();
	}

	@Override
	public List<KeyComparison> read(List<? extends String> keys) throws Exception {
		List<KeyComparison> comparisons = new ArrayList<>();
		List<DataStructure<String>> leftItems = leftReader.read(keys);
		List<DataStructure<String>> rightItems = rightReader.read(keys);
		Assert.isTrue(rightItems != null && rightItems.size() == keys.size(),
				"Missing values in value reader response");
		for (int index = 0; index < keys.size(); index++) {
			DataStructure<String> leftItem = leftItems.get(index);
			DataStructure<String> rightItem = rightItems.get(index);
			comparisons.add(new KeyComparison(leftItem, rightItem, compare(leftItem, rightItem)));
		}
		return comparisons;
	}

	private Status compare(DataStructure<String> left, DataStructure<String> right) {
		if (!Objects.equals(left.getType(), right.getType())) {
			if (DataStructure.isNone(right)) {
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
		return Math.abs(source - target) <= ttlTolerance.toMillis();
	}

}
