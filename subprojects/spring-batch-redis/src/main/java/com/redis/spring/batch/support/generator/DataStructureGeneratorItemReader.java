package com.redis.spring.batch.support.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.Range;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructure.Type;

import lombok.Data;

public abstract class DataStructureGeneratorItemReader<T>
		extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private final Type type;
	private final DataStructureOptions options;

	protected DataStructureGeneratorItemReader(Type type, DataStructureOptions options) {
		Assert.notNull(type, "A data structure type is required");
		Assert.notNull(options, "Options are required");
		setName(ClassUtils.getShortName(getClass()));
		setMaxItemCount(Math.toIntExact(options.getSequence().getMaximum() - options.getSequence().getMinimum()));
		this.type = type;
		this.options = options;
	}

	@Override
	protected DataStructure<String> doRead() throws Exception {
		String key = key();
		T value = value();
		DataStructure<String> dataStructure = new DataStructure<>(type.name(), key, value);
		if (options.getExpiration() != null) {
			dataStructure.setAbsoluteTTL(System.currentTimeMillis() + randomLong(options.getExpiration()));
		}
		return dataStructure;
	}

	private String key() {
		return type.name().toLowerCase() + ":" + prefix(String.valueOf(index()));
	}

	private String prefix(String id) {
		if (options.getKeyPrefix() == null) {
			return id;
		}
		return options.getKeyPrefix() + ":" + id;
	}

	protected long index() {
		return options.getSequence().getMinimum() + getCurrentItemCount();
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

	public long randomLong(Range<Long> range) {
		if (range.getMinimum() == range.getMaximum()) {
			return range.getMinimum();
		}
		return ThreadLocalRandom.current().nextLong(range.getMinimum(), range.getMaximum());
	}

	public double randomDouble(Range<Double> range) {
		if (range.getMinimum() == range.getMaximum()) {
			return range.getMinimum();
		}
		return ThreadLocalRandom.current().nextDouble(range.getMinimum(), range.getMaximum());
	}

	@Data
	public static class DataStructureOptions {

		private String keyPrefix;
		private Range<Long> sequence;
		private Range<Long> expiration;

		public static DataStructureOptionsBuilder<?> builder() {
			return new DataStructureOptionsBuilder<>();
		}

		@SuppressWarnings("unchecked")
		public static class DataStructureOptionsBuilder<B extends DataStructureOptionsBuilder<B>> {

			private String keyPrefix;
			private Range<Long> sequence = Generator.DEFAULT_SEQUENCE;
			private Range<Long> expiration;

			public B keyPrefix(String keyPrefix) {
				this.keyPrefix = keyPrefix;
				return (B) this;
			}

			public B sequence(Range<Long> sequence) {
				this.sequence = sequence;
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
				options.setSequence(sequence);
				options.setExpiration(expiration);
			}
		}

	}

}
