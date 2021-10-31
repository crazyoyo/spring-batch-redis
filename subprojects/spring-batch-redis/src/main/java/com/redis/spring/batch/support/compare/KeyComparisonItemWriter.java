package com.redis.spring.batch.support.compare;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.Utils;
import com.redis.spring.batch.support.compare.KeyComparison.Status;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {

	private final ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader;
	private final long ttlTolerance;
	private final Map<Status, AtomicLong> counts = Arrays.stream(Status.values())
			.collect(Collectors.toMap(Function.identity(), r -> new AtomicLong()));
	private List<KeyComparisonListener<K>> listeners = new ArrayList<>();

	public KeyComparisonItemWriter(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader,
			Duration ttlTolerance) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(valueReader, "A value reader is required");
		Utils.assertPositive(ttlTolerance, "TTL tolerance");
		this.valueReader = valueReader;
		this.ttlTolerance = ttlTolerance.toMillis();
	}

	public void addListener(KeyComparisonListener<K> listener) {
		this.listeners.add(listener);
	}

	public void setListeners(List<KeyComparisonListener<K>> listeners) {
		this.listeners = listeners;
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
			KeyComparison<K> comparison = new KeyComparison<>(source, target, status);
			counts.get(comparison.getStatus()).incrementAndGet();
			listeners.forEach(c -> c.keyComparison(comparison));
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
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

		public KeyComparisonItemWriterBuilder(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader) {
			this.valueReader = valueReader;
		}

		public KeyComparisonItemWriter<K> build() {
			return new KeyComparisonItemWriter<>(valueReader, ttlTolerance);
		}
	}

	public KeyComparisonResults getResults() {
		Map<Status, Long> results = new HashMap<>();
		counts.forEach((k, v) -> results.put(k, v.get()));
		return new KeyComparisonResults(results);
	}

}
