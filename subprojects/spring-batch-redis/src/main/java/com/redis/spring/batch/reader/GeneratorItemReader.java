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
import com.redis.spring.batch.reader.GeneratorReaderOptions.CollectionOptions;
import com.redis.spring.batch.reader.GeneratorReaderOptions.MapOptions;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class GeneratorItemReader extends AbstractItemCountingItemStreamItemReader<DataStructure<String>> {

	private final Log log = LogFactory.getLog(getClass());

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	private final ObjectMapper mapper = new ObjectMapper();
	private final Random random = new Random();
	private final GeneratorReaderOptions options;
	private final Type[] types;

	public GeneratorItemReader(GeneratorReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		setCurrentItemCount(currentItemCount(options));
		setMaxItemCount(options.getKeyRange().getMax() - currentItemCount(options));
		this.options = options;
		this.types = options.getTypes().toArray(new Type[0]);
	}

	private static int currentItemCount(GeneratorReaderOptions options) {
		return options.getKeyRange().getMin() - 1;
	}

	private String key() {
		int index = index(options.getKeyRange());
		return options.getKeyspace() + ":" + index;
	}

	private int index(IntRange range) {
		return range.getMin() + index() % (range.getMax() - range.getMin() + 1);
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
	protected DataStructure<String> doRead() throws Exception {
		DataStructure<String> ds = new DataStructure<>();
		Type type = types[index() % options.getTypes().size()];
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
