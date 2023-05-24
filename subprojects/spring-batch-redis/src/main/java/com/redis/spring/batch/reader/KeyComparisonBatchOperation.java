package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.BatchAsyncOperation;
import com.redis.spring.batch.common.ConvertingRedisFuture;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.SimpleBatchAsyncOperation;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonBatchOperation
		extends OperationItemStreamSupport<String, String, String, DataStructure<String>>
		implements BatchAsyncOperation<String, String, String, KeyComparison> {

	private final BatchAsyncOperation<String, String, String, DataStructure<String>> left;
	private final Duration ttlTolerance;

	public KeyComparisonBatchOperation(AbstractRedisClient left, AbstractRedisClient right,
			PoolOptions rightPoolOptions, Duration ttlTolerance) {
		super(right, StringCodec.UTF8, rightPoolOptions,
				new SimpleBatchAsyncOperation<>(new DataStructureStringOperation(right)));
		this.left = addDelegate(new SimpleBatchAsyncOperation<>(new DataStructureStringOperation(left)));
		this.ttlTolerance = ttlTolerance;
	}

	@Override
	public List<RedisFuture<KeyComparison>> execute(BaseRedisAsyncCommands<String, String> commands,
			List<? extends String> items) {
		List<RedisFuture<DataStructure<String>>> leftItems = left.execute(commands, items);
		List<DataStructure<String>> rightItems;
		try {
			rightItems = process(items);
		} catch (Exception e) {
			throw new ItemStreamException("Could not read from right Redis", e);
		}
		Assert.isTrue(rightItems != null && rightItems.size() == leftItems.size(),
				"Missing values in value reader response");
		List<RedisFuture<KeyComparison>> results = new ArrayList<>();
		for (int index = 0; index < items.size(); index++) {
			RedisFuture<DataStructure<String>> leftItem = leftItems.get(index);
			DataStructure<String> rightItem = rightItems.get(index);
			results.add(
					new ConvertingRedisFuture<>(leftItem, t -> new KeyComparison(t, rightItem, compare(t, rightItem))));
		}
		return results;
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
