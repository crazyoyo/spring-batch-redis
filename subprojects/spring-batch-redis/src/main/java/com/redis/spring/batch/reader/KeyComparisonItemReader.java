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

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.BaseBuilder;
import com.redis.spring.batch.common.ItemStreamProcessor;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.KeyComparison.Status;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends AbstractItemStreamItemReader<KeyComparison> {

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

	private final RedisItemReader<String, String> left;
	private final RedisItemReader<String, String> right;

	private int chunkSize = ReaderOptions.DEFAULT_CHUNK_SIZE;
	private Iterator<KeyComparison> iterator = Collections.emptyIterator();

	private ItemStreamProcessor<List<String>, List<KeyValue<String>>> rightOperationProcessor;

	public KeyComparisonItemReader(RedisItemReader<String, String> left, RedisItemReader<String, String> right) {
		this.left = left;
		this.right = right;
		setName(ClassUtils.getShortName(getClass()));
	}

	public RedisItemReader<String, String> getLeft() {
		return left;
	}

	public RedisItemReader<String, String> getRight() {
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
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (!isOpen()) {
			rightOperationProcessor = right.operationProcessor();
			rightOperationProcessor.open(executionContext);
			left.open(executionContext);
		}
	}

	public boolean isOpen() {
		return rightOperationProcessor != null && Utils.isOpen(rightOperationProcessor) && left.isOpen();
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		rightOperationProcessor.update(executionContext);
		left.update(executionContext);
	}

	@Override
	public synchronized void close() {
		if (isOpen()) {
			left.close();
			rightOperationProcessor.close();
		}
		super.close();
	}

	@Override
	public synchronized KeyComparison read() throws Exception {
		if (isOpen()) {
			return doRead();
		}
		return null;
	}

	private KeyComparison doRead() throws Exception {
		if (iterator.hasNext()) {
			return iterator.next();
		}
		List<KeyValue<String>> leftItems = readChunk();
		List<String> keys = leftItems.stream().map(KeyValue::getKey).collect(Collectors.toList());
		List<KeyValue<String>> rightItems = rightOperationProcessor.process(keys);
		List<KeyComparison> results = new ArrayList<>();
		for (int index = 0; index < leftItems.size(); index++) {
			KeyValue<String> leftItem = leftItems.get(index);
			KeyValue<String> rightItem = getElement(rightItems, index);
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

	private List<KeyValue<String>> readChunk() throws Exception {
		List<KeyValue<String>> items = new ArrayList<>();
		KeyValue<String> item;
		while (items.size() < chunkSize && (item = left.read()) != null) {
			items.add(item);
		}
		return items;
	}

	private Status compare(KeyValue<String> left, KeyValue<String> right) {
		if (right == null) {
			return Status.MISSING;
		}
		if (!Objects.equals(left.getType(), right.getType())) {
			if (KeyValue.isNone(right)) {
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

	public static Builder builder(AbstractRedisClient left, AbstractRedisClient right) {
		return new Builder(left, right);
	}

	public static class Builder extends BaseBuilder<String, String, Builder> {

		private final AbstractRedisClient right;
		private Duration ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE;
		private ReaderOptions rightOptions = ReaderOptions.builder().build();

		public Builder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left, StringCodec.UTF8);
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
			RedisItemReader<String, String> leftReader = reader(client).struct();
			RedisItemReader<String, String> rightReader = reader(right).options(rightOptions).struct();
			KeyComparisonItemReader reader = new KeyComparisonItemReader(leftReader, rightReader);
			reader.setTtlTolerance(ttlTolerance);
			return reader;
		}

		private RedisItemReader.Builder<String, String> reader(AbstractRedisClient client) {
			return toBuilder(new RedisItemReader.Builder<>(client, StringCodec.UTF8));
		}

	}

}
