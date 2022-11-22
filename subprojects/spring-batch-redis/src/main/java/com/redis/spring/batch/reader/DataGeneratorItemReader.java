package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.reader.DataGeneratorOptions.CollectionOptions;
import com.redis.spring.batch.reader.DataGeneratorOptions.MapOptions;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataGeneratorItemReader extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private final Log log = LogFactory.getLog(getClass());

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	private final ObjectMapper mapper = new ObjectMapper();
	private final Random random = new Random();
	private final DataGeneratorOptions options;
	private final List<Type> types;

	public DataGeneratorItemReader(DataGeneratorOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		setCurrentItemCount(options.getKeyRange().getMin());
		setMaxItemCount(options.getKeyRange().getMax() - options.getKeyRange().getMin());
		this.options = options;
		this.types = new ArrayList<>(options.getTypes());
	}

	public String key() {
		return options.getKeyspace() + ":" + getCurrentItemCount();
	}

	private Object value(Type type) {
		switch (type) {
		case HASH:
			return map(options.getHashOptions());
		case LIST:
			return members(options.getListOptions());
		case SET:
			return new HashSet<>(members(options.getSetOptions()));
		case STREAM:
			return streamMessages();
		case STRING:
			return string(options.getStringOptions().getLength());
		case ZSET:
			return zset();
		case JSON:
			try {
				return mapper.writeValueAsString(map(options.getJsonOptions()));
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
		int size = randomInt(options.getTimeSeriesOptions().getSampleCount());
		for (int index = 0; index < size; index++) {
			long time = options.getTimeSeriesOptions().getStartTime() + index() + index;
			samples.add(Sample.of(time, random.nextDouble()));
		}
		return samples;
	}

	private List<ScoredValue<String>> zset() {
		return members(options.getZsetOptions()).stream()
				.map(m -> ScoredValue.just(randomDouble(options.getZsetOptions().getScore()), m))
				.collect(Collectors.toList());
	}

	private Collection<StreamMessage<String, String>> streamMessages() {
		String key = key();
		Collection<StreamMessage<String, String>> messages = new ArrayList<>();
		for (int elementIndex = 0; elementIndex < randomInt(
				options.getStreamOptions().getMessageCount()); elementIndex++) {
			messages.add(new StreamMessage<>(key, null, map(options.getStreamOptions().getBodyOptions())));
		}
		return messages;
	}

	public Map<String, String> map(MapOptions options) {
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
		for (int index = 0; index < randomInt(options.getMemberCount()); index++) {
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

	@Override
	protected DataStructure<String> doRead() throws Exception {
		DataStructure<String> ds = new DataStructure<>();
		Type type = types.get(index() % options.getTypes().size());
		ds.setType(type);
		ds.setKey(key());
		ds.setValue(value(type));
		options.getExpiration().ifPresent(e -> ds.setTtl(System.currentTimeMillis() + randomInt(e)));
		return ds;
	}

	private int index() {
		return getCurrentItemCount() - 1;
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
