package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonValueReader extends ItemStreamSupport implements ValueReader<String, KeyComparison> {

	private final DataStructureValueReader<String, String> leftReader;
	private final DataStructureValueReader<String, String> rightReader;
	private final KeyComparatorOptions options;

	public KeyComparisonValueReader(AbstractRedisClient left, AbstractRedisClient right, KeyComparatorOptions options) {
		this.leftReader = new DataStructureValueReader<>(left, StringCodec.UTF8, options.getLeftPoolOptions());
		this.rightReader = new DataStructureValueReader<>(right, StringCodec.UTF8, options.getRightPoolOptions());
		this.options = options;
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
		List<DataStructure<String>> leftItems;
		try {
			leftItems = leftReader.read(keys);
		} catch (Exception e) {
			throw new ExecutionException("Could not read keys from left", e);
		}
		List<DataStructure<String>> rightItems;
		try {
			rightItems = rightReader.read(keys);
		} catch (Exception e) {
			throw new ExecutionException("Could not read keys from right", e);
		}
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
		return Math.abs(source - target) <= options.getTtlTolerance().toMillis();
	}

}
