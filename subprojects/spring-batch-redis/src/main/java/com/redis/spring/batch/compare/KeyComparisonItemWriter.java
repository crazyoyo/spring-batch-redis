package com.redis.spring.batch.compare;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.compare.KeyComparison.Status;
import com.redis.spring.batch.reader.ValueReader;
import com.redis.spring.batch.support.Utils;

public class KeyComparisonItemWriter extends AbstractItemStreamItemWriter<DataStructure<String>> {

	private final Log log = LogFactory.getLog(getClass());

	private final KeyComparisonResults results = new KeyComparisonResults();
	private final ValueReader<String, DataStructure<String>> valueReader;
	private final long ttlTolerance;
	private List<KeyComparisonListener> listeners = new ArrayList<>();

	public KeyComparisonItemWriter(ValueReader<String, DataStructure<String>> valueReader, Duration ttlTolerance) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(valueReader, "A value reader is required");
		Utils.assertPositive(ttlTolerance, "TTL tolerance");
		this.valueReader = valueReader;
		this.ttlTolerance = ttlTolerance.toMillis();
	}

	public void addListener(KeyComparisonListener listener) {
		this.listeners.add(listener);
	}

	public void setListeners(List<KeyComparisonListener> listeners) {
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
	public void write(List<? extends DataStructure<String>> sourceItems) throws Exception {
		List<DataStructure<String>> targetItems = valueReader
				.read(sourceItems.stream().map(DataStructure::getKey).collect(Collectors.toList()));
		if (targetItems == null || targetItems.size() != sourceItems.size()) {
			log.warn("Missing values in value reader response");
			return;
		}
		results.addAndGetSource(sourceItems.size());
		for (int index = 0; index < sourceItems.size(); index++) {
			DataStructure<String> source = sourceItems.get(index);
			DataStructure<String> target = targetItems.get(index);
			Status status = compare(source, target);
			increment(status);
			KeyComparison comparison = new KeyComparison(source, target, status);
			listeners.forEach(c -> c.keyComparison(comparison));
		}
	}

	private long increment(Status status) {
		switch (status) {
		case OK:
			return results.incrementOK();
		case MISSING:
			return results.incrementMissing();
		case TTL:
			return results.incrementTTL();
		case TYPE:
			return results.incrementType();
		case VALUE:
			return results.incrementValue();
		}
		throw new IllegalArgumentException("Unknown status: " + status);
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
		return Math.abs(source - target) <= ttlTolerance;
	}

	public KeyComparisonResults getResults() {
		return results;
	}

	public static Builder valueReader(ValueReader<String, DataStructure<String>> valueReader) {
		return new Builder(valueReader);
	}

	public static class Builder {

		private static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

		private final ValueReader<String, DataStructure<String>> valueReader;
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

		public Builder(ValueReader<String, DataStructure<String>> valueReader) {
			this.valueReader = valueReader;
		}

		public Builder tolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public KeyComparisonItemWriter build() {
			return new KeyComparisonItemWriter(valueReader, ttlTolerance);
		}
	}

}
