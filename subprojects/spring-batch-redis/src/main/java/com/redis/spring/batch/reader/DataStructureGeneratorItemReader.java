package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.support.DoubleRange;
import com.redis.spring.batch.support.IntRange;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructureGeneratorItemReader extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private static final Logger log = LoggerFactory.getLogger(DataStructureGeneratorItemReader.class);

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	public static final String DEFAULT_KEYSPACE = "gen";
	public static final int DEFAULT_COUNT = 100;
	public static final int DEFAULT_START = 1;
	public static final IntRange DEFAULT_HASH_SIZE = IntRange.is(10);
	public static final IntRange DEFAULT_HASH_FIELD_SIZE = IntRange.is(100);
	public static final IntRange DEFAULT_STREAM_FIELD_COUNT = IntRange.is(10);
	public static final IntRange DEFAULT_STREAM_FIELD_SIZE = IntRange.is(100);
	public static final IntRange DEFAULT_JSON_FIELD_COUNT = IntRange.is(10);
	public static final IntRange DEFAULT_JSON_FIELD_SIZE = IntRange.is(100);
	public static final IntRange DEFAULT_TIMESERIES_SIZE = IntRange.is(10);
	public static final IntRange DEFAULT_STREAM_SIZE = IntRange.is(10);
	public static final IntRange DEFAULT_ZSET_SIZE = IntRange.is(10);
	public static final IntRange DEFAULT_SET_SIZE = IntRange.is(10);
	public static final IntRange DEFAULT_LIST_SIZE = IntRange.is(10);
	public static final IntRange DEFAULT_STRING_SIZE = IntRange.is(100);
	public static final DoubleRange DEFAULT_ZSET_SCORE = DoubleRange.between(0, 100);
	private static final List<Type> DEFAULT_TYPES = Arrays.asList(Type.HASH, Type.LIST, Type.SET, Type.STREAM,
			Type.STRING, Type.ZSET);

	private final ObjectMapper mapper = new ObjectMapper();
	private final int count;
	private final Optional<IntRange> expiration;
	private final IntRange hashSize;
	private final IntRange hashFieldSize;
	private final IntRange streamFieldCount;
	private final IntRange streamFieldSize;
	private final IntRange jsonFieldCount;
	private final IntRange jsonFieldSize;
	private final IntRange timeseriesSize;
	private final IntRange streamSize;
	private final IntRange listSize;
	private final IntRange setSize;
	private final IntRange zsetSize;
	private final IntRange stringSize;
	private final DoubleRange zsetScore;
	private final String keyspace;

	private final Random random = new Random();
	private final EnumMap<Type, AtomicInteger> indexes = new EnumMap<>(Type.class);
	private final Type[] types;
	private int typeIndex = 0;

	public String key(Type type, int index) {
		return keyspace + ":" + type.name().toLowerCase() + ":" + index;
	}

	private Object value(Type type) {
		switch (type) {
		case HASH:
			return map(hashSize, hashFieldSize);
		case LIST:
			return members(listSize);
		case SET:
			return new HashSet<>(members(setSize));
		case STREAM:
			return streamMessages();
		case STRING:
			return string(stringSize);
		case ZSET:
			return zset();
		case JSON:
			try {
				return mapper.writeValueAsString(map(jsonFieldCount, jsonFieldSize));
			} catch (JsonProcessingException e) {
				log.error("Could not serialize object to JSON", e);
				return null;
			}
		case TIMESERIES:
			return samples();
		case NONE:
		case UNKNOWN:
			return null;
		}
		throw new IllegalArgumentException("Unsupported type " + type);
	}

	private List<Sample> samples() {
		List<Sample> samples = new ArrayList<>();
		int size = randomInt(timeseriesSize);
		long start = System.currentTimeMillis() - count * size;
		for (int index = 0; index < size; index++) {
			samples.add(Sample.of(start + getCurrentItemCount() + index, random.nextDouble()));
		}
		return samples;
	}

	private List<ScoredValue<String>> zset() {
		return members(zsetSize).stream().map(m -> ScoredValue.just(randomDouble(zsetScore), m))
				.collect(Collectors.toList());
	}

	private Collection<StreamMessage<String, String>> streamMessages() {
		String key = key(Type.STREAM, indexes.get(Type.STREAM).get());
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < randomInt(streamSize); elementIndex++) {
			messages.add(new StreamMessage<>(key, null, map(streamFieldCount, streamFieldSize)));
		}
		return messages;
	}

	public Map<String, String> map(IntRange fieldCount, IntRange fieldSize) {
		Map<String, String> hash = new HashMap<>();
		for (int index = 0; index < randomInt(fieldCount); index++) {
			int fieldIndex = index + 1;
			hash.put("field" + fieldIndex, string(fieldSize));
		}
		return hash;
	}

	private String string(IntRange range) {
		int length = range.getMin() + random.nextInt((range.getMax() - range.getMin()) + 1);
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private List<String> members(IntRange size) {
		List<String> members = new ArrayList<>();
		for (int index = 0; index < randomInt(size); index++) {
			members.add(String.valueOf(index));
		}
		return members;
	}

	private int randomInt(IntRange range) {
		if (range.getMin() == range.getMax()) {
			return range.getMin();
		}
		return ThreadLocalRandom.current().nextInt(range.getMin(), range.getMax());
	}

	private double randomDouble(DoubleRange range) {
		if (range.getMin() == range.getMax()) {
			return range.getMin();
		}
		return ThreadLocalRandom.current().nextDouble(range.getMin(), range.getMax());
	}

	private DataStructureGeneratorItemReader(Builder builder) {
		setName(ClassUtils.getShortName(getClass()));
		setMaxItemCount(builder.count);
		this.keyspace = builder.keyspace;
		this.types = builder.types.toArray(Type[]::new);
		builder.types.forEach(type -> indexes.put(type, new AtomicInteger(builder.start)));
		this.count = builder.count;
		this.expiration = builder.expiration;
		this.hashSize = builder.hashSize;
		this.hashFieldSize = builder.hashFieldSize;
		this.streamFieldCount = builder.streamFieldCount;
		this.streamFieldSize = builder.streamFieldSize;
		this.jsonFieldCount = builder.jsonFieldCount;
		this.jsonFieldSize = builder.jsonFieldSize;
		this.setSize = builder.setSize;
		this.streamSize = builder.streamSize;
		this.zsetSize = builder.zsetSize;
		this.listSize = builder.listSize;
		this.timeseriesSize = builder.timeseriesSize;
		this.stringSize = builder.stringSize;
		this.zsetScore = builder.zsetScore;
	}

	@Override
	protected DataStructure<String> doRead() throws Exception {
		DataStructure<String> ds = new DataStructure<>();
		Type type = types[typeIndex++ % types.length];
		ds.setType(type);
		int index = indexes.get(type).getAndIncrement();
		ds.setKey(key(type, index));
		ds.setValue(value(type));
		expiration.ifPresent(e -> ds.setTtl(System.currentTimeMillis() + randomInt(e)));
		return ds;
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private String keyspace = DEFAULT_KEYSPACE;
		private List<Type> types = DEFAULT_TYPES;
		private int count = DEFAULT_COUNT;
		private int start = DEFAULT_START;
		private Optional<IntRange> expiration = Optional.empty();
		private IntRange hashSize = DEFAULT_HASH_SIZE;
		private IntRange hashFieldSize = DEFAULT_HASH_FIELD_SIZE;
		private IntRange streamFieldCount = DEFAULT_STREAM_FIELD_COUNT;
		private IntRange streamFieldSize = DEFAULT_STREAM_FIELD_SIZE;
		private IntRange jsonFieldCount = DEFAULT_JSON_FIELD_COUNT;
		private IntRange jsonFieldSize = DEFAULT_JSON_FIELD_SIZE;
		private IntRange streamSize = DEFAULT_STREAM_SIZE;
		private IntRange timeseriesSize = DEFAULT_TIMESERIES_SIZE;
		private IntRange setSize = DEFAULT_SET_SIZE;
		private IntRange zsetSize = DEFAULT_ZSET_SIZE;
		private IntRange listSize = DEFAULT_LIST_SIZE;
		private IntRange stringSize = DEFAULT_STRING_SIZE;
		private DoubleRange zsetScore = DEFAULT_ZSET_SCORE;

		public Builder keyspace(String keyspace) {
			this.keyspace = keyspace;
			return this;
		}

		public Builder count(int count) {
			Assert.isTrue(count > 0, "Count must be strictly positive");
			this.count = count;
			return this;
		}

		public Builder start(int start) {
			Assert.isTrue(start > 0, "Start must be strictly positive");
			this.start = start;
			return this;
		}

		public Builder expiration(IntRange expiration) {
			this.expiration = Optional.of(expiration);
			return this;
		}

		public Builder hashSize(IntRange size) {
			this.hashSize = size;
			return this;
		}

		public Builder hashFieldSize(IntRange hashFieldSize) {
			this.hashFieldSize = hashFieldSize;
			return this;
		}

		public Builder streamFieldCount(IntRange streamFieldCount) {
			this.streamFieldCount = streamFieldCount;
			return this;
		}

		public Builder streamFieldSize(IntRange streamFieldSize) {
			this.streamFieldSize = streamFieldSize;
			return this;
		}

		public Builder jsonFieldCount(IntRange jsonFieldCount) {
			this.jsonFieldCount = jsonFieldCount;
			return this;
		}

		public Builder jsonFieldSize(IntRange jsonFieldSize) {
			this.jsonFieldSize = jsonFieldSize;
			return this;
		}

		public Builder setSize(IntRange size) {
			this.setSize = size;
			return this;
		}

		public Builder zsetSize(IntRange size) {
			this.zsetSize = size;
			return this;
		}

		public Builder streamSize(IntRange size) {
			this.streamSize = size;
			return this;
		}

		public Builder listSize(IntRange size) {
			this.listSize = size;
			return this;
		}

		public Builder timeseriesSize(IntRange size) {
			this.timeseriesSize = size;
			return this;
		}

		public Builder stringSize(IntRange stringSize) {
			this.stringSize = stringSize;
			return this;
		}

		public Builder zsetScore(DoubleRange zsetScore) {
			this.zsetScore = zsetScore;
			return this;
		}

		public Builder types(Type... types) {
			this.types = Arrays.asList(types);
			return this;
		}

		public DataStructureGeneratorItemReader build() {
			return new DataStructureGeneratorItemReader(this);
		}
	}

	public static List<Type> defaultTypes() {
		return DEFAULT_TYPES;
	}

}
