package com.redis.spring.batch.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.generator.Generator.Type;

public abstract class DataStructureGeneratorItemReader<T>
		extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private final Type type;
	private Range<Long> sequence = Generator.DEFAULT_SEQUENCE;
	private Optional<Range<Long>> expiration = Optional.empty();

	protected DataStructureGeneratorItemReader(Type type) {
		Assert.notNull(type, "A data structure type is required");
		setName(ClassUtils.getShortName(getClass()));
		setMaxItemCount();
		this.type = type;
	}

	public void setSequence(Range<Long> sequence) {
		this.sequence = sequence;
		setMaxItemCount();
	}

	public void setExpiration(Optional<Range<Long>> expiration) {
		this.expiration = expiration;
	}

	public void setExpiration(Range<Long> expiration) {
		this.expiration = Optional.of(expiration);
	}

	private void setMaxItemCount() {
		setMaxItemCount(Math.toIntExact(sequence.getMaximum() - sequence.getMinimum()));
	}

	@Override
	protected DataStructure<String> doRead() throws Exception {
		String key = key();
		T value = value();
		DataStructure<String> dataStructure = new DataStructure<>(key, value, type.name().toLowerCase());
		expiration.ifPresent(e -> dataStructure.setTtl(System.currentTimeMillis() + randomLong(e)));
		return dataStructure;
	}

	private String key() {
		return type.name().toLowerCase() + ":" + index();
	}

	protected long index() {
		return sequence.getMinimum() + getCurrentItemCount();
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
		if (range.getMinimum().equals(range.getMaximum())) {
			return range.getMinimum();
		}
		return ThreadLocalRandom.current().nextLong(range.getMinimum(), range.getMaximum());
	}

	public double randomDouble(Range<Double> range) {
		if (range.getMinimum().equals(range.getMaximum())) {
			return range.getMinimum();
		}
		return ThreadLocalRandom.current().nextDouble(range.getMinimum(), range.getMaximum());
	}

}
