package com.redis.spring.batch.support.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.Range;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.generator.Generator.DataType;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DataStructureGeneratorItemReader<T>
		extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	protected final Random random = new Random();
	private final Options options;
	private final DataType type;

	protected DataStructureGeneratorItemReader(Options options, DataType type) {
		setName(ClassUtils.getShortName(getClass()));
		setMaxItemCount(options.getInterval().getMaximum() - options.getInterval().getMinimum());
		this.options = options;
		this.type = type;
	}

	@Override
	protected DataStructure<String> doRead() throws Exception {
		String key = key(type, index());
		T value = value();
		DataStructure<String> dataStructure = new DataStructure<>(type.name(), key, value);
		if (options.getExpiration() != null) {
			dataStructure.setAbsoluteTTL(System.currentTimeMillis() + options.getExpiration().getMinimum()
					+ random.nextInt(Math.toIntExact(options.getExpiration().getMaximum())));
		}
		return dataStructure;
	}

	private String key(DataType type, int index) {
		return key(type, String.valueOf(index));
	}

	private String key(DataType type, String id) {
		return type + ":" + prefix(id);
	}

	private String prefix(String id) {
		if (options.getKeyPrefix() == null) {
			return id;
		}
		return options.getKeyPrefix() + ":" + id;
	}

	protected int index() {
		return options.getInterval().getMinimum() + getCurrentItemCount();
	}

	protected abstract T value();

	@Override
	public void close() throws ItemStreamException {
		log.info("Closing reader. Item count: {}", getCurrentItemCount());
		super.close();
	}

	@Override
	protected void doOpen() throws Exception {
	}

	@Override
	protected void doClose() throws Exception {
	}

	protected Map<String, String> map() {
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		return hash;
	}

	public int random(Range<Integer> range) {
		int interval = range.getMaximum() - range.getMinimum();
		if (interval == 0) {
			return range.getMinimum();
		}
		return range.getMinimum() + random.nextInt(interval);
	}

	@Data
	@Builder
	public static class Options {

		public static final int DEFAULT_START = 0;
		public static final int DEFAULT_END = 100;

		private String keyPrefix;
		@Default
		private Range<Integer> interval = Range.between(DEFAULT_START, DEFAULT_END);
		private Range<Long> expiration;

		public void to(int max) {
			this.interval = Range.between(interval.getMinimum(), max);
		}

	}

}
