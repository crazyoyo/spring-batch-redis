package com.redis.spring.batch.memcached;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.Range;

public class MemcachedGeneratorItemReader extends AbstractItemCountingItemStreamItemReader<MemcachedEntry> {

	public static final String DEFAULT_KEYSPACE = "gen";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Range DEFAULT_KEY_RANGE = Range.from(1);
	public static final Range DEFAULT_STRING_LENGTH = Range.of(100);

	private static final Random random = new Random();

	private String keySeparator = DEFAULT_KEY_SEPARATOR;
	private String keyspace = DEFAULT_KEYSPACE;
	private Range keyRange = DEFAULT_KEY_RANGE;
	private Range expiration;
	private Range stringLength = DEFAULT_STRING_LENGTH;

	private int startTime;

	public MemcachedGeneratorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	private String key() {
		int index = keyRange.getMin() + (getCurrentItemCount() - 1) % (keyRange.getMax() - keyRange.getMin());
		return key(index);
	}

	public String key(int index) {
		StringBuilder builder = new StringBuilder();
		builder.append(keyspace);
		builder.append(keySeparator);
		builder.append(index);
		return builder.toString();
	}

	private byte[] value() {
		int length = randomInt(stringLength);
		byte[] value = new byte[length];
		random.nextBytes(value);
		return value;
	}

	private int randomInt(Range range) {
		if (range.getMin() == range.getMax()) {
			return range.getMin();
		}
		return ThreadLocalRandom.current().nextInt(range.getMin(), range.getMax());
	}

	@Override
	protected void doOpen() throws Exception {
		startTime = Math.toIntExact(System.currentTimeMillis() / 1000);
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	@Override
	protected MemcachedEntry doRead() {
		MemcachedEntry struct = new MemcachedEntry();
		struct.setKey(key());
		struct.setValue(value());
		if (expiration != null) {
			struct.setExpiration(startTime + randomInt(expiration));
		}
		return struct;
	}

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public void setKeyRange(Range range) {
		this.keyRange = range;
	}

	public Range getKeyRange() {
		return keyRange;
	}

	public Range getExpiration() {
		return expiration;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setExpiration(Range range) {
		this.expiration = range;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Range getStringLength() {
		return stringLength;
	}

	public void setStringLength(Range range) {
		this.stringLength = range;
	}

}
