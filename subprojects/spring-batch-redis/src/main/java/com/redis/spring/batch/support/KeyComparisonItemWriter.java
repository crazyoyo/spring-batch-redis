package com.redis.spring.batch.support;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {

	public enum Status {
		OK, // No difference
		MISSING, // Key missing in target database
		TYPE, // Type mismatch
		TTL, // TTL mismatch
		VALUE // Value mismatch
	}

	public static final Set<Status> MISMATCHES = new HashSet<>(
			Arrays.asList(Status.MISSING, Status.TYPE, Status.TTL, Status.VALUE));

	public interface KeyComparisonResultHandler<K> {

		void accept(DataStructure<K> source, DataStructure<K> target, Status status);

	}

	private final ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader;
	private final long ttlTolerance;
	private final List<KeyComparisonResultHandler<K>> resultHandlers;

	public KeyComparisonItemWriter(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader,
			Duration ttlTolerance, List<KeyComparisonResultHandler<K>> resultHandlers) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(valueReader, "A value reader is required");
		Utils.assertPositive(ttlTolerance, "TTL tolerance");
		Assert.notEmpty(resultHandlers, "At least one result handler is required");
		this.valueReader = valueReader;
		this.ttlTolerance = ttlTolerance.toMillis();
		this.resultHandlers = resultHandlers;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).open(executionContext);
		}
		super.open(executionContext);
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).update(executionContext);
		}
		super.update(executionContext);
	}

	@Override
	public void close() {
		super.close();
		if (valueReader instanceof ItemStream) {
			((ItemStream) valueReader).close();
		}
	}

	@Override
	public void write(List<? extends DataStructure<K>> sourceItems) throws Exception {
		List<DataStructure<K>> targetItems = valueReader
				.process(sourceItems.stream().map(DataStructure::getKey).collect(Collectors.toList()));
		if (targetItems == null || targetItems.size() != sourceItems.size()) {
			log.warn("Missing values in value reader response");
			return;
		}
		for (int index = 0; index < sourceItems.size(); index++) {
			DataStructure<K> source = sourceItems.get(index);
			DataStructure<K> target = targetItems.get(index);
			Status status = compare(source, target);
			for (KeyComparisonResultHandler<K> handler : resultHandlers) {
				handler.accept(source, target, status);
			}
		}
	}

	private Status compare(DataStructure<K> source, DataStructure<K> target) {
		if (DataStructure.NONE.equalsIgnoreCase(source.getType())) {
			if (DataStructure.NONE.equalsIgnoreCase(target.getType())) {
				return Status.OK;
			}
			return Status.TYPE;
		}
		if (DataStructure.NONE.equalsIgnoreCase(target.getType())) {
			return Status.MISSING;
		}
		if (!ObjectUtils.nullSafeEquals(source.getType(), target.getType())) {
			return Status.TYPE;
		}
		if (source.getValue() == null) {
			if (target.getValue() == null) {
				return Status.OK;
			}
			return Status.VALUE;
		}
		if (target.getValue() == null) {
			return Status.MISSING;
		}
		if (Objects.deepEquals(source.getValue(), target.getValue())) {
			if (source.hasTTL()) {
				if (target.hasTTL() && Math.abs(source.getAbsoluteTTL() - target.getAbsoluteTTL()) <= ttlTolerance) {
					return Status.OK;
				}
				return Status.TTL;
			}
			if (target.hasTTL()) {
				return Status.TTL;
			}
			return Status.OK;
		}
		return Status.VALUE;
	}

	public static <K> KeyComparisonItemWriterBuilder<K> valueReader(
			ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader) {
		return new KeyComparisonItemWriterBuilder<>(valueReader);
	}

	@Setter
	@Accessors(fluent = true)
	public static class KeyComparisonItemWriterBuilder<K> {

		private static final Duration DEFAULT_TTL_TOLERANCE = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL
				.multipliedBy(2);

		private final ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader;
		private final List<KeyComparisonResultHandler<K>> resultHandlers = new ArrayList<>();
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

		public KeyComparisonItemWriterBuilder(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader) {
			this.valueReader = valueReader;
		}

		public KeyComparisonItemWriterBuilder<K> resultHandler(KeyComparisonResultHandler<K> resultHandler) {
			resultHandlers.add(resultHandler);
			return this;
		}

		@SuppressWarnings("unchecked")
		public KeyComparisonItemWriterBuilder<K> resultHandlers(KeyComparisonResultHandler<K>... resultHandlers) {
			this.resultHandlers.addAll(Arrays.asList(resultHandlers));
			return this;
		}

		public KeyComparisonItemWriter<K> build() {
			return new KeyComparisonItemWriter<>(valueReader, ttlTolerance, resultHandlers);
		}
	}

}
