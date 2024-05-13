package com.redis.spring.batch.item.redis.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractItemCountingItemStreamItemReader<KeyValue<String, Object>> {

	public static final String DEFAULT_KEYSPACE = "gen";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Range DEFAULT_KEY_RANGE = Range.from(1);
	protected static final List<DataType> DEFAULT_TYPES = Arrays.asList(DataType.HASH, DataType.JSON, DataType.LIST,
			DataType.SET, DataType.STREAM, DataType.STRING, DataType.TIMESERIES, DataType.ZSET);

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'
	private static final Random random = new Random();
	private final ObjectMapper mapper = new ObjectMapper();

	private String keySeparator = DEFAULT_KEY_SEPARATOR;
	private String keyspace = DEFAULT_KEYSPACE;
	private Range keyRange = DEFAULT_KEY_RANGE;
	private Range expiration;
	private MapOptions hashOptions = new MapOptions();
	private StreamOptions streamOptions = new StreamOptions();
	private TimeSeriesOptions timeSeriesOptions = new TimeSeriesOptions();
	private MapOptions jsonOptions = new MapOptions();
	private CollectionOptions listOptions = new CollectionOptions();
	private CollectionOptions setOptions = new CollectionOptions();
	private StringOptions stringOptions = new StringOptions();
	private ZsetOptions zsetOptions = new ZsetOptions();
	private List<DataType> types = DEFAULT_TYPES;
	private int maxItemCount;

	public GeneratorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public static List<DataType> defaultTypes() {
		return DEFAULT_TYPES;
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

	public MapOptions getHashOptions() {
		return hashOptions;
	}

	public StreamOptions getStreamOptions() {
		return streamOptions;
	}

	public TimeSeriesOptions getTimeSeriesOptions() {
		return timeSeriesOptions;
	}

	public MapOptions getJsonOptions() {
		return jsonOptions;
	}

	public CollectionOptions getListOptions() {
		return listOptions;
	}

	public CollectionOptions getSetOptions() {
		return setOptions;
	}

	public StringOptions getStringOptions() {
		return stringOptions;
	}

	public ZsetOptions getZsetOptions() {
		return zsetOptions;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public List<DataType> getTypes() {
		return types;
	}

	public void setExpiration(Range range) {
		this.expiration = range;
	}

	public void setHashOptions(MapOptions options) {
		this.hashOptions = options;
	}

	public void setStreamOptions(StreamOptions options) {
		this.streamOptions = options;
	}

	public void setJsonOptions(MapOptions options) {
		this.jsonOptions = options;
	}

	public void setTimeSeriesOptions(TimeSeriesOptions options) {
		this.timeSeriesOptions = options;
	}

	public void setListOptions(CollectionOptions options) {
		this.listOptions = options;
	}

	public void setSetOptions(CollectionOptions options) {
		this.setOptions = options;
	}

	public void setZsetOptions(ZsetOptions options) {
		this.zsetOptions = options;
	}

	public void setStringOptions(StringOptions options) {
		this.stringOptions = options;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setTypes(DataType... types) {
		setTypes(Arrays.asList(types));
	}

	public void setTypes(List<DataType> types) {
		this.types = types;
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

	private Object value(DataType type) throws JsonProcessingException {
		switch (type) {
		case HASH:
			return hash();
		case LIST:
			return list();
		case SET:
			return set();
		case STREAM:
			return streamMessages();
		case STRING:
			return string();
		case ZSET:
			return zset();
		case JSON:
			return json();
		case TIMESERIES:
			return samples();
		default:
			return null;
		}
	}

	private String json() throws JsonProcessingException {
		return mapper.writeValueAsString(map(jsonOptions));
	}

	private Map<String, String> hash() {
		return map(hashOptions);
	}

	private String string() {
		return string(stringOptions.getLength());
	}

	private List<String> list() {
		return members(listOptions).collect(Collectors.toList());
	}

	private Set<String> set() {
		return members(setOptions).collect(Collectors.toSet());
	}

	private List<Sample> samples() {
		List<Sample> samples = new ArrayList<>();
		int size = randomInt(timeSeriesOptions.getSampleCount());
		long startTime = timeSeriesStartTime();
		for (int index = 0; index < size; index++) {
			long time = startTime + getCurrentItemCount() + index;
			samples.add(Sample.of(time, random.nextDouble()));
		}
		return samples;
	}

	private long timeSeriesStartTime() {
		return timeSeriesOptions.getStartTime().toEpochMilli();
	}

	private Set<ScoredValue<String>> zset() {
		return members(zsetOptions).map(this::scoredValue).collect(Collectors.toSet());
	}

	private ScoredValue<String> scoredValue(String value) {
		double score = randomDouble(zsetOptions.getScore());
		return ScoredValue.just(score, value);
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

	private String string(Range range) {
		int length = randomInt(range);
		return string(length);
	}

	public static String string(int length) {
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private Stream<String> members(CollectionOptions options) {
		int spread = options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1;
		return IntStream.range(0, randomInt(options.getMemberCount()))
				.map(i -> options.getMemberRange().getMin() + i % spread).mapToObj(String::valueOf);
	}

	private int randomInt(Range range) {
		if (range.getMin() == range.getMax()) {
			return range.getMin();
		}
		return ThreadLocalRandom.current().nextInt(range.getMin(), range.getMax());
	}

	private double randomDouble(Range range) {
		if (range.getMin() == range.getMax()) {
			return range.getMin();
		}
		return ThreadLocalRandom.current().nextDouble(range.getMin(), range.getMax());
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	@Override
	protected KeyValue<String, Object> doRead() throws JsonProcessingException {
		KeyValue<String, Object> struct = new KeyValue<>();
		struct.setKey(key());
		DataType type = types.get(getCurrentItemCount() % types.size());
		struct.setType(type.getString());
		struct.setValue(value(type));
		if (expiration != null) {
			struct.setTtl(ttl());
		}
		return struct;
	}

	private long ttl() {
		return System.currentTimeMillis() + randomInt(expiration);
	}

	@Override
	public void setMaxItemCount(int count) {
		super.setMaxItemCount(count);
		this.maxItemCount = count;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

}
