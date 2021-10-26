package com.redis.spring.batch.support.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.Range;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.generator.Generator.DataType;

import lombok.Data;

public abstract class DataStructureGeneratorItemReader<T>
		extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	protected final Random random = new Random();
	private final DataType type;
	private final DataStructureOptions options;

	protected DataStructureGeneratorItemReader(DataType type, DataStructureOptions options) {
		Assert.notNull(type, "A data structure type is required");
		Assert.notNull(options, "Options are required");
		setName(ClassUtils.getShortName(getClass()));
		setMaxItemCount(Math.toIntExact(options.getEnd() - options.getStart()));
		this.type = type;
		this.options = options;
	}

	@Override
	protected DataStructure<String> doRead() throws Exception {
		String key = key();
		T value = value();
		DataStructure<String> dataStructure = new DataStructure<>(type.name(), key, value);
		if (options.getExpiration() != null) {
			dataStructure.setAbsoluteTTL(System.currentTimeMillis() + options.getExpiration().getMinimum()
					+ random.nextInt(Math.toIntExact(options.getExpiration().getMaximum())));
		}
		return dataStructure;
	}

	private String key() {
		return type + ":" + prefix(String.valueOf(index()));
	}

	private String prefix(String id) {
		if (options.getKeyPrefix() == null) {
			return id;
		}
		return options.getKeyPrefix() + ":" + id;
	}

	protected long index() {
		return options.getStart() + getCurrentItemCount();
	}

	protected abstract T value();

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
	public static class DataStructureOptions {

		private String keyPrefix;
		private long start;
		private long end;
		private Range<Long> expiration;

		public static DataStructureOptionsBuilder<?> builder() {
			return new DataStructureOptionsBuilder<>();
		}

		@SuppressWarnings("unchecked")
		public static class DataStructureOptionsBuilder<B extends DataStructureOptionsBuilder<B>> {

			public static final int DEFAULT_START = 0;
			public static final int DEFAULT_END = 100;

			private String keyPrefix;
			private long start = DEFAULT_START;
			private long end = DEFAULT_END;
			private Range<Long> expiration;

			public B keyPrefix(String keyPrefix) {
				this.keyPrefix = keyPrefix;
				return (B) this;
			}

			public B start(long start) {
				Assert.isTrue(start <= end, "Start must be lower than end (" + end + ")");
				this.start = start;
				return (B) this;
			}

			public B end(long end) {
				Assert.isTrue(end >= start, "End must be greater than start (" + start + ")");
				this.end = end;
				return (B) this;
			}

			public B expiration(Range<Long> expiration) {
				this.expiration = expiration;
				return (B) this;
			}

			public DataStructureOptions build() {
				DataStructureOptions options = new DataStructureOptions();
				set(options);
				return options;
			}

			protected void set(DataStructureOptions options) {
				options.setKeyPrefix(keyPrefix);
				options.setStart(start);
				options.setEnd(end);
				options.setExpiration(expiration);
			}
		}

	}

}
