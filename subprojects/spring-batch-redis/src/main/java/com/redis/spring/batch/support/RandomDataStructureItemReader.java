package com.redis.spring.batch.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class RandomDataStructureItemReader extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private static final Logger log = LoggerFactory.getLogger(RandomDataStructureItemReader.class);

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	public static final String DEFAULT_KEYSPACE = "generated:";
	public static final Range<Integer> DEFAULT_SEQUENCE = Range.between(0, 100);
	public static final Range<Integer> DEFAULT_COLLECTION_CARDINALITY = Range.is(10);
	public static final Range<Integer> DEFAULT_STRING_VALUE_SIZE = Range.is(100);
	public static final Range<Double> DEFAULT_ZSET_SCORE = Range.between(0D, 100D);
	private static final Type[] DEFAULT_TYPES = { Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET };

	private final ObjectMapper mapper = new ObjectMapper();
	private final Random random = new Random();
	private final Supplier<Type> randomTypeSupplier;
	private final Range<Integer> sequence;
	private final Optional<Range<Integer>> expiration;
	private final Range<Integer> collectionCardinality;
	private final Range<Integer> stringSize;
	private final Range<Double> zsetScore;
	private final String keyspace;

	private RandomDataStructureItemReader(Builder builder) {
		setName(ClassUtils.getShortName(getClass()));
		setMaxItemCount(builder.sequence.getMaximum() - builder.sequence.getMinimum());
		this.keyspace = builder.keyspace;
		this.randomTypeSupplier = () -> builder.types[random.nextInt(builder.types.length)];
		this.sequence = builder.sequence;
		this.expiration = builder.expiration;
		this.collectionCardinality = builder.collectionCardinality;
		this.stringSize = builder.stringSize;
		this.zsetScore = builder.zsetScore;
	}

	@Override
	protected DataStructure<String> doRead() throws Exception {
		DataStructure<String> dataStructure = new DataStructure<>();
		dataStructure.setKey(key());
		Type type = randomTypeSupplier.get();
		dataStructure.setType(type);
		dataStructure.setValue(value(type));
		expiration.ifPresent(e -> dataStructure.setTtl(System.currentTimeMillis() + randomInt(e)));
		return dataStructure;
	}

	private String key() {
		return keyspace + index();
	}

	private int index() {
		return sequence.getMinimum() + getCurrentItemCount();
	}

	private Object value(Type type) {
		switch (type) {
		case HASH:
			return hash();
		case LIST:
			return members();
		case SET:
			return new HashSet<>(members());
		case STREAM:
			return streamMessages();
		case STRING:
			return string();
		case ZSET:
			return zset();
		case JSON:
			try {
				return mapper.writeValueAsString(hash());
			} catch (JsonProcessingException e) {
				log.error("Could not serialize object to JSON", e);
				return null;
			}
		case TIMESERIES:
			return samples();
		case NONE:
			return null;
		}
		throw new IllegalArgumentException("Unsupported type " + type);
	}

	private List<Sample> samples() {
		List<Sample> samples = new ArrayList<>();
		long start = System.currentTimeMillis() - sequence.getMaximum() * collectionCardinality();
		for (int index = 0; index < collectionCardinality(); index++) {
			samples.add(Sample.of(start + index() + index, random.nextDouble()));
		}
		return samples;
	}

	private List<ScoredValue<String>> zset() {
		return members().stream().map(m -> ScoredValue.just(randomDouble(zsetScore), m)).collect(Collectors.toList());
	}

	private Collection<StreamMessage<String, String>> streamMessages() {
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < collectionCardinality(); elementIndex++) {
			messages.add(new StreamMessage<>(key(), null, hash()));
		}
		return messages;
	}

	private Map<String, String> hash() {
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value1");
		hash.put("field2", "value2");
		return hash;
	}

	private String string() {
		int length = stringSize.getMinimum() + random.nextInt((stringSize.getMaximum() - stringSize.getMinimum()) + 1);
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private int collectionCardinality() {
		return randomInt(collectionCardinality);
	}

	private List<String> members() {
		List<String> members = new ArrayList<>();
		for (int index = 0; index < collectionCardinality(); index++) {
			members.add("member:" + index);
		}
		return members;
	}

	private int randomInt(Range<Integer> range) {
		if (range.getMinimum().equals(range.getMaximum())) {
			return range.getMinimum();
		}
		return ThreadLocalRandom.current().nextInt(range.getMinimum(), range.getMaximum());
	}

	private double randomDouble(Range<Double> range) {
		if (range.getMinimum().equals(range.getMaximum())) {
			return range.getMinimum();
		}
		return ThreadLocalRandom.current().nextDouble(range.getMinimum(), range.getMaximum());
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
		private Type[] types = DEFAULT_TYPES;
		private Range<Integer> sequence = DEFAULT_SEQUENCE;
		private Optional<Range<Integer>> expiration = Optional.empty();
		private Range<Integer> collectionCardinality = DEFAULT_COLLECTION_CARDINALITY;
		private Range<Integer> stringSize = DEFAULT_STRING_VALUE_SIZE;
		private Range<Double> zsetScore = DEFAULT_ZSET_SCORE;

		public Builder keyspace(String keyspace) {
			this.keyspace = keyspace;
			return this;
		}

		public Builder sequence(Range<Integer> sequence) {
			this.sequence = sequence;
			return this;
		}

		public Builder expiration(Range<Integer> expiration) {
			this.expiration = Optional.of(expiration);
			return this;
		}

		public Builder collectionCardinality(Range<Integer> collectionCardinality) {
			this.collectionCardinality = collectionCardinality;
			return this;
		}

		public Builder stringValueSize(Range<Integer> stringValueSize) {
			this.stringSize = stringValueSize;
			return this;
		}

		public Builder zsetScore(Range<Double> zsetScore) {
			this.zsetScore = zsetScore;
			return this;
		}

		public Builder types(Type... types) {
			this.types = types;
			return this;
		}

		public Builder end(int end) {
			sequence(Range.between(0, end));
			return this;
		}

		public Builder between(int start, int end) {
			sequence(Range.between(start, end));
			return this;
		}

		public RandomDataStructureItemReader build() {
			return new RandomDataStructureItemReader(this);
		}
	}

	public static List<Type> defaultTypes() {
		return Arrays.asList(DEFAULT_TYPES);
	}

}
