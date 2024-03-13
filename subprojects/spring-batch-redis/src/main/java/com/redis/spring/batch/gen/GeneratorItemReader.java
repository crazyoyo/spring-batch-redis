package com.redis.spring.batch.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Range;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractItemCountingItemStreamItemReader<KeyValue<String>> {

	public static final String DEFAULT_KEYSPACE = "gen";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Range DEFAULT_KEY_RANGE = Range.from(1);

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
	private List<DataType> types = defaultTypes();

	public GeneratorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public static List<DataType> defaultTypes() {
		return Arrays.asList(DataType.HASH, DataType.JSON, DataType.LIST, DataType.SET, DataType.STREAM,
				DataType.STRING, DataType.TIMESERIES, DataType.ZSET);
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
		int index = keyRange.getMin() + index() % keyRange.getSpread();
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
			return map(hashOptions);
		case LIST:
			return members(listOptions);
		case SET:
			return new HashSet<>(members(setOptions));
		case STREAM:
			return streamMessages();
		case STRING:
			return string(stringOptions.getLength());
		case ZSET:
			return zset();
		case JSON:
			return mapper.writeValueAsString(map(jsonOptions));
		case TIMESERIES:
			return samples();
		default:
			return null;
		}
	}

	private List<Sample> samples() {
		List<Sample> samples = new ArrayList<>();
		long size = randomLong(timeSeriesOptions.getSampleCount());
		long startTime = timeSeriesStartTime();
		for (int index = 0; index < size; index++) {
			long time = startTime + index() + index;
			samples.add(Sample.of(time, random.nextDouble()));
		}
		return samples;
	}

	private long timeSeriesStartTime() {
		return timeSeriesOptions.getStartTime().toEpochMilli();
	}

	private List<ScoredValue<String>> zset() {
		return members(zsetOptions).stream().map(this::scoredValue).collect(Collectors.toList());
	}

	private ScoredValue<String> scoredValue(String value) {
		double score = randomDouble(zsetOptions.getScore());
		return ScoredValue.just(score, value);
	}

	private Collection<StreamMessage<String, String>> streamMessages() {
		String key = key();
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < randomLong(streamOptions.getMessageCount()); elementIndex++) {
			messages.add(new StreamMessage<>(key, null, map(streamOptions.getBodyOptions())));
		}
		return messages;
	}

	private Map<String, String> map(MapOptions options) {
		Map<String, String> hash = new HashMap<>();
		for (int index = 0; index < randomLong(options.getFieldCount()); index++) {
			int fieldIndex = index + 1;
			hash.put("field" + fieldIndex, string(options.getFieldLength()));
		}
		return hash;
	}

	private String string(Range range) {
		long length = randomLong(range);
		return string(length);
	}

	public static String string(long length) {
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	private List<String> members(CollectionOptions options) {
		List<String> members = new ArrayList<>();
		int spread = options.getMemberRange().getMax() - options.getMemberRange().getMin() + 1;
		for (int index = 0; index < randomLong(options.getMemberCount()); index++) {
			int memberId = options.getMemberRange().getMin() + index % spread;
			members.add(String.valueOf(memberId));
		}
		return members;
	}

	private long randomLong(Range range) {
		if (range.getMin() == range.getMax()) {
			return range.getMin();
		}
		return ThreadLocalRandom.current().nextLong(range.getMin(), range.getMax());
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
	protected KeyValue<String> doRead() throws JsonProcessingException {
		KeyValue<String> struct = new KeyValue<>();
		struct.setKey(key());
		DataType type = types.get(index() % types.size());
		struct.setType(type);
		struct.setValue(value(type));
		if (expiration != null) {
			struct.setTtl(ttl());
		}
		return struct;
	}

	private long ttl() {
		return System.currentTimeMillis() + randomLong(expiration);
	}

	private int index() {
		return getCurrentItemCount() - 1;
	}

}
