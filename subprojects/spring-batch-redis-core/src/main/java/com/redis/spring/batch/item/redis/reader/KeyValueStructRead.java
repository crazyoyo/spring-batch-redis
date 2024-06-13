package com.redis.spring.batch.item.redis.reader;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class KeyValueStructRead<K, V> extends KeyValueRead<K, V, Object> {

	public KeyValueStructRead(RedisCodec<K, V> codec) {
		super(ValueType.STRUCT, codec);
	}

	@Override
	public KeyValue<K, Object> convert(List<Object> list) {
		KeyValue<K, Object> struct = super.convert(list);
		struct.setValue(value(struct));
		return struct;
	}

	private Object value(KeyValue<K, Object> struct) {
		if (!KeyValue.hasValue(struct)) {
			return null;
		}
		DataType type = KeyValue.type(struct);
		if (type == null) {
			return struct.getValue();
		}
		switch (type) {
		case HASH:
			return hash(struct);
		case SET:
			return set(struct);
		case STREAM:
			return stream(struct);
		case TIMESERIES:
			return timeseries(struct);
		case ZSET:
			return zset(struct);
		default:
			return struct.getValue();
		}
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> hash(KeyValue<K, Object> struct) {
		return map((List<Object>) struct.getValue());
	}

	@SuppressWarnings("unchecked")
	private List<Sample> timeseries(KeyValue<K, Object> struct) {
		List<List<Object>> items = (List<List<Object>>) struct.getValue();
		return items.stream().map(this::sample).collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	private Set<V> set(KeyValue<K, Object> struct) {
		return new HashSet<>((Collection<V>) struct.getValue());
	}

	@SuppressWarnings("unchecked")
	private List<StreamMessage<K, V>> stream(KeyValue<K, Object> struct) {
		Collection<List<Object>> items = (Collection<List<Object>>) struct.getValue();
		return items.stream().map(this::message).collect(Collectors.toList());
	}

	private Sample sample(List<Object> sample) {
		LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
		Long timestamp = (Long) sample.get(0);
		return Sample.of(timestamp, toDouble(sample.get(1)));
	}

	private double toDouble(Object value) {
		return Double.parseDouble(toString(value));
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> map(List<Object> list) {
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i < list.size(); i += 2) {
			map.put((K) list.get(i), (V) list.get(i + 1));
		}
		return map;
	}

	@SuppressWarnings("unchecked")
	private Set<ScoredValue<V>> zset(KeyValue<K, Object> struct) {
		List<Object> list = (List<Object>) struct.getValue();
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Set<ScoredValue<V>> values = new HashSet<>();
		for (int i = 0; i < list.size(); i += 2) {
			double score = toDouble(list.get(i + 1));
			values.add(ScoredValue.just(score, (V) list.get(i)));
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private StreamMessage<K, V> message(List<Object> message) {
		LettuceAssert.isTrue(message.size() == 2, "Invalid list size: " + message.size());
		String id = toString(message.get(0));
		Map<K, V> body = map((List<Object>) message.get(1));
		return new StreamMessage<>(null, id, body);

	}

}
