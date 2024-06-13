package com.redis.spring.batch.item.redis.gen;

import java.util.ArrayList;
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

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'
	private static final Random random = new Random();
	private final ObjectMapper mapper = new ObjectMapper();

	private GeneratorOptions options = new GeneratorOptions();

	public GeneratorItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	private String key() {
		return key(index(options.getKeyRange()));
	}

	private int index(Range range) {
		return range.getMin() + (getCurrentItemCount() - 1) % (range.getMax() - range.getMin());
	}

	public String key(int index) {
		StringBuilder builder = new StringBuilder();
		builder.append(options.getKeyspace());
		builder.append(options.getKeySeparator());
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
		return mapper.writeValueAsString(map(options.getJsonOptions()));
	}

	private Map<String, String> hash() {
		return map(options.getHashOptions());
	}

	private String string() {
		return string(options.getStringOptions().getLength());
	}

	private List<String> list() {
		return members(options.getListOptions()).collect(Collectors.toList());
	}

	private Set<String> set() {
		return members(options.getSetOptions()).collect(Collectors.toSet());
	}

	private List<Sample> samples() {
		List<Sample> samples = new ArrayList<>();
		int size = randomInt(options.getTimeSeriesOptions().getSampleCount());
		long startTime = timeSeriesStartTime();
		for (int index = 0; index < size; index++) {
			long time = startTime + getCurrentItemCount() + index;
			samples.add(Sample.of(time, random.nextDouble()));
		}
		return samples;
	}

	private long timeSeriesStartTime() {
		return options.getTimeSeriesOptions().getStartTime().toEpochMilli();
	}

	private Set<ScoredValue<String>> zset() {
		return members(options.getZsetOptions()).map(this::scoredValue).collect(Collectors.toSet());
	}

	private ScoredValue<String> scoredValue(String value) {
		double score = randomDouble(options.getZsetOptions().getScore());
		return ScoredValue.just(score, value);
	}

	private Collection<StreamMessage<String, String>> streamMessages() {
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < randomInt(
				options.getStreamOptions().getMessageCount()); elementIndex++) {
			messages.add(new StreamMessage<>(null, null, map(options.getStreamOptions().getBodyOptions())));
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
		DataType type = options.getTypes().get(getCurrentItemCount() % options.getTypes().size());
		struct.setType(type.getString());
		struct.setValue(value(type));
		if (options.getExpiration() != null) {
			struct.setTtl(ttl());
		}
		return struct;
	}

	private long ttl() {
		return System.currentTimeMillis() + randomInt(options.getExpiration());
	}

	public GeneratorOptions getOptions() {
		return options;
	}

	public void setOptions(GeneratorOptions options) {
		this.options = options;
	}
}
