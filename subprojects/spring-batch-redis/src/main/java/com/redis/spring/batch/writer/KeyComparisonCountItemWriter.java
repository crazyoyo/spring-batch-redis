package com.redis.spring.batch.writer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;

public class KeyComparisonCountItemWriter extends AbstractItemStreamItemWriter<KeyComparison> implements Openable {

	private final Results results = new Results();
	private boolean open;

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		this.open = true;
	}

	@Override
	public void close() {
		super.close();
		this.open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public void write(List<? extends KeyComparison> items) throws Exception {
		for (KeyComparison comparison : items) {
			results.incrementAndGet(comparison.getStatus());
		}
	}

	public Results getResults() {
		return results;
	}

	public static class Results {

		private final Map<Status, AtomicLong> counts = Stream.of(Status.values())
				.collect(Collectors.toMap(s -> s, s -> new AtomicLong()));

		public long incrementAndGet(Status status) {
			return counts.get(status).incrementAndGet();
		}

		public long getCount(Status status) {
			return counts.get(status).get();
		}

		public long getTotalCount() {
			return counts.values().stream().collect(Collectors.summingLong(AtomicLong::get));
		}

	}

}
