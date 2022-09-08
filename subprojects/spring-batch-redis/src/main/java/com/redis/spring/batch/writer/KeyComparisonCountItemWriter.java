package com.redis.spring.batch.writer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;

public class KeyComparisonCountItemWriter<K> implements ItemWriter<KeyComparison<K>> {

	private final Results results = new Results();

	@Override
	public void write(List<? extends KeyComparison<K>> items) throws Exception {
		for (KeyComparison<K> comparison : items) {
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
