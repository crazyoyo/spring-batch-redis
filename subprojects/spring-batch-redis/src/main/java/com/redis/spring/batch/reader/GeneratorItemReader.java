package com.redis.spring.batch.reader;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private static final Log log = LogFactory.getLog(GeneratorItemReader.class);

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	private final ObjectMapper mapper = new ObjectMapper();
	private final Random random = new Random();

	public enum Type {
		HASH, STRING, LIST, SET, ZSET, JSON, STREAM, TIMESERIES
	}

	public static final String DEFAULT_KEYSPACE = "gen";
	public static final HashOptions DEFAULT_HASH_OPTIONS = HashOptions.builder().build();
	public static final StreamOptions DEFAULT_STREAM_OPTIONS = StreamOptions.builder().build();
	public static final TimeSeriesOptions DEFAULT_TIMESERIES_OPTIONS = TimeSeriesOptions.builder().build();
	public static final JsonOptions DEFAULT_JSON_OPTIONS = JsonOptions.builder().build();
	public static final SetOptions DEFAULT_SET_OPTIONS = SetOptions.builder().build();
	public static final ListOptions DEFAULT_LIST_OPTIONS = ListOptions.builder().build();
	public static final StringOptions DEFAULT_STRING_OPTIONS = StringOptions.builder().build();
	public static final ZsetOptions DEFAULT_ZSET_OPTIONS = ZsetOptions.builder().build();
	private static final Type[] DEFAULT_TYPES = { Type.HASH, Type.LIST, Type.SET, Type.STREAM, Type.STRING, Type.ZSET };
	public static final int DEFAULT_KEY_RANGE_MAX = 100;
	public static final IntRange DEFAULT_KEY_RANGE = IntRange.between(1, DEFAULT_KEY_RANGE_MAX);

	private Optional<IntRange> expiration = Optional.empty();
	private HashOptions hashOptions = DEFAULT_HASH_OPTIONS;
	private StreamOptions streamOptions = DEFAULT_STREAM_OPTIONS;
	private TimeSeriesOptions timeSeriesOptions = DEFAULT_TIMESERIES_OPTIONS;
	private JsonOptions jsonOptions = DEFAULT_JSON_OPTIONS;
	private ListOptions listOptions = DEFAULT_LIST_OPTIONS;
	private SetOptions setOptions = DEFAULT_SET_OPTIONS;
	private StringOptions stringOptions = DEFAULT_STRING_OPTIONS;
	private ZsetOptions zsetOptions = DEFAULT_ZSET_OPTIONS;
	private String keyspace = DEFAULT_KEYSPACE;
	private List<Type> types = defaultTypes();
	private IntRange keyRange = DEFAULT_KEY_RANGE;

	public GeneratorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public static List<Type> defaultTypes() {
		return Stream.of(DEFAULT_TYPES).collect(Collectors.toList());
	}

	public GeneratorItemReader withKeyRange(IntRange keyRange) {
		this.keyRange = keyRange;
		return this;
	}

	public GeneratorItemReader withExpiration(IntRange expiration) {
		return withExpiration(Optional.of(expiration));
	}

	public GeneratorItemReader withExpiration(Optional<IntRange> expiration) {
		this.expiration = expiration;
		return this;
	}

	public GeneratorItemReader withHashOptions(HashOptions hashOptions) {
		this.hashOptions = hashOptions;
		return this;
	}

	public GeneratorItemReader withStreamOptions(StreamOptions streamOptions) {
		this.streamOptions = streamOptions;
		return this;
	}

	public GeneratorItemReader withJsonOptions(JsonOptions jsonOptions) {
		this.jsonOptions = jsonOptions;
		return this;
	}

	public GeneratorItemReader withTimeSeriesOptions(TimeSeriesOptions options) {
		this.timeSeriesOptions = options;
		return this;
	}

	public GeneratorItemReader withListOptions(ListOptions options) {
		this.listOptions = options;
		return this;
	}

	public GeneratorItemReader withSetOptions(SetOptions options) {
		this.setOptions = options;
		return this;
	}

	public GeneratorItemReader withZsetOptions(ZsetOptions options) {
		this.zsetOptions = options;
		return this;
	}

	public GeneratorItemReader withStringOptions(StringOptions options) {
		this.stringOptions = options;
		return this;//
	}

	public GeneratorItemReader withKeyspace(String keyspace) {
		this.keyspace = keyspace;
		return this;
	}

	public GeneratorItemReader withTypes(Type... types) {
		return withTypes(Arrays.asList(types));
	}

	public GeneratorItemReader withTypes(List<Type> types) {
		this.types = types;
		return this;
	}

	public static class ZsetOptions extends CollectionOptions {

		public static final DoubleRange DEFAULT_SCORE = DoubleRange.between(0, 100);

		private DoubleRange score = DEFAULT_SCORE;

		private ZsetOptions(Builder builder) {
			super(builder);
			this.score = builder.score;
		}

		public DoubleRange getScore() {
			return score;
		}

		public void setScore(DoubleRange score) {
			this.score = score;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder extends CollectionOptions.Builder<Builder> {

			private DoubleRange score = DEFAULT_SCORE;

			private Builder() {
			}

			public Builder score(DoubleRange score) {
				this.score = score;
				return this;
			}

			public ZsetOptions build() {
				return new ZsetOptions(this);
			}
		}

	}

	public static class TimeSeriesOptions {

		public static final IntRange DEFAULT_SAMPLE_COUNT = IntRange.is(10);

		private IntRange sampleCount = DEFAULT_SAMPLE_COUNT;
		private long startTime;

		private TimeSeriesOptions(Builder builder) {
			this.sampleCount = builder.sampleCount;
			this.startTime = builder.startTime;
		}

		public IntRange getSampleCount() {
			return sampleCount;
		}

		public void setSampleCount(IntRange sampleCount) {
			this.sampleCount = sampleCount;
		}

		public long getStartTime() {
			return startTime;
		}

		public void setStartTime(long startTime) {
			this.startTime = startTime;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {
			private IntRange sampleCount = DEFAULT_SAMPLE_COUNT;
			private long startTime;

			private Builder() {
			}

			public Builder sampleCount(IntRange sampleCount) {
				this.sampleCount = sampleCount;
				return this;
			}

			public Builder startTime(long startTime) {
				this.startTime = startTime;
				return this;
			}

			public TimeSeriesOptions build() {
				return new TimeSeriesOptions(this);
			}
		}
	}

	public static class StreamOptions {

		public static final IntRange DEFAULT_MESSAGE_COUNT = IntRange.is(10);
		private static final BodyOptions DEFAULT_BODY_OPTIONS = BodyOptions.builder().build();

		private IntRange messageCount = DEFAULT_MESSAGE_COUNT;
		private BodyOptions bodyOptions = DEFAULT_BODY_OPTIONS;

		private StreamOptions(Builder builder) {
			this.messageCount = builder.messageCount;
			this.bodyOptions = builder.bodyOptions;
		}

		public IntRange getMessageCount() {
			return messageCount;
		}

		public void setMessageCount(IntRange count) {
			this.messageCount = count;
		}

		public MapOptions getBodyOptions() {
			return bodyOptions;
		}

		public void setBodyOptions(BodyOptions bodyOptions) {
			this.bodyOptions = bodyOptions;
		}

		public static class BodyOptions extends MapOptions {

			private BodyOptions(Builder builder) {
				super(builder);
			}

			public static Builder builder() {
				return new Builder();
			}

			public static class Builder extends MapOptions.Builder<Builder> {

				@Override
				public BodyOptions build() {
					return new BodyOptions(this);
				}
			}
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private IntRange messageCount = DEFAULT_MESSAGE_COUNT;
			private BodyOptions bodyOptions = DEFAULT_BODY_OPTIONS;

			private Builder() {
			}

			public Builder messageCount(IntRange count) {
				this.messageCount = count;
				return this;
			}

			public Builder messageCount(int count) {
				return messageCount(IntRange.is(count));
			}

			public Builder bodyOptions(BodyOptions options) {
				this.bodyOptions = options;
				return this;
			}

			public StreamOptions build() {
				return new StreamOptions(this);
			}
		}

	}

	public static class CollectionOptions {

		public static final IntRange DEFAULT_MEMBER_RANGE = IntRange.between(1, 100);
		public static final IntRange DEFAULT_CARDINALITY = IntRange.is(100);

		private IntRange memberRange = DEFAULT_MEMBER_RANGE;
		private IntRange cardinality = DEFAULT_CARDINALITY;

		protected CollectionOptions(Builder<?> builder) {
			this.memberRange = builder.memberRange;
			this.cardinality = builder.cardinality;
		}

		public IntRange getMemberRange() {
			return memberRange;
		}

		public void setMemberRange(IntRange range) {
			this.memberRange = range;
		}

		public IntRange getCardinality() {
			return cardinality;
		}

		public void setCardinality(IntRange cardinality) {
			this.cardinality = cardinality;
		}

		public static class Builder<B extends Builder<B>> {

			private IntRange memberRange = DEFAULT_MEMBER_RANGE;
			private IntRange cardinality = DEFAULT_CARDINALITY;

			@SuppressWarnings("unchecked")
			public B memberRange(IntRange range) {
				this.memberRange = range;
				return (B) this;
			}

			@SuppressWarnings("unchecked")
			public B cardinality(IntRange cardinality) {
				this.cardinality = cardinality;
				return (B) this;
			}

		}

	}

	public static class ListOptions extends CollectionOptions {

		private ListOptions(Builder builder) {
			super(builder);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static class Builder extends CollectionOptions.Builder<Builder> {

			public ListOptions build() {
				return new ListOptions(this);
			}

		}

	}

	public static class SetOptions extends CollectionOptions {

		private SetOptions(Builder builder) {
			super(builder);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static class Builder extends CollectionOptions.Builder<Builder> {

			public SetOptions build() {
				return new SetOptions(this);
			}

		}

	}

	public static class StringOptions {

		public static final IntRange DEFAULT_LENGTH = IntRange.is(100);

		private IntRange length = DEFAULT_LENGTH;

		private StringOptions(Builder builder) {
			this.length = builder.length;
		}

		public IntRange getLength() {
			return length;
		}

		public void setLength(IntRange length) {
			this.length = length;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private IntRange length = DEFAULT_LENGTH;

			private Builder() {
			}

			public Builder length(IntRange length) {
				this.length = length;
				return this;
			}

			public StringOptions build() {
				return new StringOptions(this);
			}
		}

	}

	public static class HashOptions extends MapOptions {

		private HashOptions(Builder builder) {
			super(builder);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static class Builder extends MapOptions.Builder<Builder> {

			@Override
			public HashOptions build() {
				return new HashOptions(this);
			}
		}
	}

	public static class JsonOptions extends MapOptions {

		private JsonOptions(Builder builder) {
			super(builder);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static class Builder extends MapOptions.Builder<Builder> {

			@Override
			public JsonOptions build() {
				return new JsonOptions(this);
			}
		}
	}

	public static class MapOptions {

		public static final IntRange DEFAULT_FIELD_COUNT = IntRange.is(10);
		public static final IntRange DEFAULT_FIELD_LENGTH = IntRange.is(100);

		private IntRange fieldCount = DEFAULT_FIELD_COUNT;
		private IntRange fieldLength = DEFAULT_FIELD_LENGTH;

		protected MapOptions(Builder<?> builder) {
			this.fieldCount = builder.fieldCount;
			this.fieldLength = builder.fieldLength;
		}

		public IntRange getFieldCount() {
			return fieldCount;
		}

		public void setFieldCount(IntRange count) {
			this.fieldCount = count;
		}

		public IntRange getFieldLength() {
			return fieldLength;
		}

		public void setFieldLength(IntRange length) {
			this.fieldLength = length;
		}

		public static class Builder<B extends Builder<B>> {

			private IntRange fieldCount = DEFAULT_FIELD_COUNT;
			private IntRange fieldLength = DEFAULT_FIELD_LENGTH;

			private Builder() {
			}

			@SuppressWarnings("unchecked")
			public B fieldCount(IntRange count) {
				this.fieldCount = count;
				return (B) this;
			}

			@SuppressWarnings("unchecked")
			public B fieldLength(IntRange length) {
				this.fieldLength = length;
				return (B) this;
			}

			public MapOptions build() {
				return new MapOptions(this);
			}
		}

	}

	private String key() {
		int index = index(keyRange);
		return keyspace + ":" + index;
	}

	private int index(IntRange range) {
		return range.getMin() + index() % (range.getMax() - range.getMin() + 1);
	}

	private Object value(DataStructure<String> ds) {
		switch (ds.getType()) {
		case DataStructure.HASH:
			return map(hashOptions);
		case DataStructure.LIST:
			return members(listOptions);
		case DataStructure.SET:
			return new HashSet<>(members(setOptions));
		case DataStructure.STREAM:
			return streamMessages();
		case DataStructure.STRING:
			return string(stringOptions.getLength());
		case DataStructure.ZSET:
			return zset();
		case DataStructure.JSON:
			try {
				return mapper.writeValueAsString(map(jsonOptions));
			} catch (JsonProcessingException e) {
				log.error("Could not serialize object to JSON", e);
				return null;
			}
		case DataStructure.TIMESERIES:
			return samples();
		default:
			return null;
		}
	}

	private List<Sample> samples() {
		List<Sample> samples = new ArrayList<>();
		int size = randomInt(timeSeriesOptions.getSampleCount());
		for (int index = 0; index < size; index++) {
			long time = timeSeriesOptions.getStartTime() + index() + index;
			samples.add(Sample.of(time, random.nextDouble()));
		}
		return samples;
	}

	private List<ScoredValue<String>> zset() {
		return members(zsetOptions).stream().map(m -> ScoredValue.just(randomDouble(zsetOptions.getScore()), m))
				.collect(Collectors.toList());
	}

	private Collection<StreamMessage<String, String>> streamMessages() {
		String key = key();
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < randomInt(streamOptions.getMessageCount()); elementIndex++) {
			messages.add(new StreamMessage<>(key, null, map(streamOptions.getBodyOptions())));
		}
		return messages;
	}

	private Map<String, String> map(MapOptions options) {
		Map<String, String> hash = new HashMap<>();
		for (int index = 0; index < randomInt(options.getFieldCount()); index++) {
			int fieldIndex = index + 1;
			hash.put("field" + fieldIndex, string(options.getFieldLength()));
		}
		return hash;
	}

	private String string(IntRange range) {
		int length = range.getMin() + random.nextInt((range.getMax() - range.getMin()) + 1);
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private List<String> members(CollectionOptions options) {
		List<String> members = new ArrayList<>();
		for (int index = 0; index < randomInt(options.getCardinality()); index++) {
			int memberId = options.getMemberRange().getMin()
					+ index % (options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1);
			members.add(String.valueOf(memberId));
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

	@Override
	protected DataStructure<String> doRead() {
		DataStructure<String> ds = new DataStructure<>();
		Type type = types.get(index() % types.size());
		ds.setType(typeString(type));
		ds.setKey(key());
		ds.setValue(value(ds));
		expiration.ifPresent(e -> ds.setTtl(System.currentTimeMillis() + randomInt(e)));
		return ds;
	}

	private String typeString(Type type) {
		switch (type) {
		case HASH:
			return DataStructure.HASH;
		case JSON:
			return DataStructure.JSON;
		case LIST:
			return DataStructure.LIST;
		case SET:
			return DataStructure.SET;
		case STREAM:
			return DataStructure.STREAM;
		case STRING:
			return DataStructure.STRING;
		case TIMESERIES:
			return DataStructure.TIMESERIES;
		case ZSET:
			return DataStructure.ZSET;
		default:
			return DataStructure.NONE;
		}
	}

	private int index() {
		return getCurrentItemCount() - 1;
	}

	@Override
	protected void doOpen() {
		// do nothing
	}

	@Override
	protected void doClose() {
		// do nothing
	}

}
