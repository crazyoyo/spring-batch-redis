package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.BaseScanBuilder;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends AbstractItemStreamItemReader<KeyComparison> implements Openable {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

	private final RedisItemReader<String, String, DataStructure<String>> left;
	private final RedisItemReader<String, String, DataStructure<String>> right;

	private int chunkSize = ReaderOptions.DEFAULT_CHUNK_SIZE;
	private OperationItemStreamSupport<String, String, String, DataStructure<String>> rightOperationProcessor;
	private Iterator<KeyComparison> iterator = Collections.emptyIterator();

	public KeyComparisonItemReader(RedisItemReader<String, String, DataStructure<String>> left,
			RedisItemReader<String, String, DataStructure<String>> right) {
		this.left = left;
		this.right = right;
		setName(ClassUtils.getShortName(getClass()));
	}

	public RedisItemReader<String, String, DataStructure<String>> getLeft() {
		return left;
	}

	public RedisItemReader<String, String, DataStructure<String>> getRight() {
		return right;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		left.setName(name + "-left");
		right.setName(name + "-right");
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		Assert.notNull(ttlTolerance, "Tolerance must not be null");
		this.ttlTolerance = ttlTolerance;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		rightOperationProcessor = right.operationProcessor();
		rightOperationProcessor.open(executionContext);
		left.open(executionContext);
	}

	@Override
	public boolean isOpen() {
		return left.isOpen() && rightOperationProcessor.isOpen();
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		rightOperationProcessor.update(executionContext);
		left.update(executionContext);
	}

	@Override
	public void close() {
		left.close();
		rightOperationProcessor.close();
		super.close();
	}

	@Override
	public synchronized KeyComparison read() throws Exception {
		if (iterator.hasNext()) {
			return iterator.next();
		}
		List<DataStructure<String>> leftItems = readChunk();
		List<String> keys = leftItems.stream().map(DataStructure::getKey).collect(Collectors.toList());
		List<DataStructure<String>> rightItems = rightOperationProcessor.process(keys);
		List<KeyComparison> results = new ArrayList<>();
		for (int index = 0; index < leftItems.size(); index++) {
			DataStructure<String> leftItem = leftItems.get(index);
			DataStructure<String> rightItem = getElement(rightItems, index);
			Status status = compare(leftItem, rightItem);
			results.add(new KeyComparison(leftItem, rightItem, status));
		}
		iterator = results.iterator();
		if (iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

	private <T> T getElement(List<T> list, int index) {
		if (list == null || index >= list.size()) {
			return null;
		}
		return list.get(index);
	}

	private List<DataStructure<String>> readChunk() throws Exception {
		List<DataStructure<String>> items = new ArrayList<>();
		DataStructure<String> item;
		while (items.size() < chunkSize && (item = left.read()) != null) {
			items.add(item);
		}
		return items;
	}

	private Status compare(DataStructure<String> left, DataStructure<String> right) {
		if (right == null) {
			return Status.MISSING;
		}
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

	public static Builder compare(RedisModulesClient left, RedisModulesClient right) {
		return new Builder(left, right);
	}

	public static Builder compare(RedisModulesClient left, RedisModulesClusterClient right) {
		return new Builder(left, right);
	}

	public static Builder compare(RedisModulesClusterClient left, RedisModulesClient right) {
		return new Builder(left, right);
	}

	public static Builder compare(RedisModulesClusterClient left, RedisModulesClusterClient right) {
		return new Builder(left, right);
	}

	public static class IntermediaryBuilder {

		private final AbstractRedisClient left;

		public IntermediaryBuilder(AbstractRedisClient left) {
			this.left = left;
		}

		public Builder right(RedisModulesClient right) {
			return new Builder(left, right);
		}

		public Builder right(RedisModulesClusterClient right) {
			return new Builder(left, right);
		}

	}

	public static class Builder extends BaseScanBuilder<Builder> {

		private final AbstractRedisClient right;
		private Duration ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE;
		private ReaderOptions rightOptions = ReaderOptions.builder().build();

		public Builder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left);
			this.right = right;
		}

		public Builder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public Builder rightOptions(ReaderOptions options) {
			this.rightOptions = options;
			return this;
		}

		public KeyComparisonItemReader build() {
			RedisItemReader<String, String, DataStructure<String>> leftReader = reader(StringCodec.UTF8,
					new StringDataStructureReadOperation(client));
			RedisItemReader<String, String, DataStructure<String>> rightReader = new ScanBuilder(right)
					.jobRepository(jobRepository).options(rightOptions).scanOptions(scanOptions).dataStructure();
			KeyComparisonItemReader reader = new KeyComparisonItemReader(leftReader, rightReader);
			reader.setTtlTolerance(ttlTolerance);
			return reader;
		}

	}

}
