package com.redis.spring.batch.item.redis.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class EvalStructFunction<K, V> extends EvalFunction<K, V, Object> {

	public EvalStructFunction(RedisCodec<K, V> codec) {
		super(codec);
	}

	@Override
	public MemKeyValue<K, Object> apply(List<Object> list) {
		MemKeyValue<K, Object> keyValue = super.apply(list);
		keyValue.setValue(value(keyValue));
		return keyValue;
	}

	private Object value(MemKeyValue<K, Object> keyValue) {
		if (!KeyValue.hasValue(keyValue)) {
			return null;
		}
		DataType type = KeyValue.type(keyValue);
		if (type == null) {
			return keyValue.getValue();
		}
		switch (type) {
		case JSON:
			return string(keyValue);
		case HASH:
			return hash(keyValue);
		case LIST:
			return members(keyValue);
		case SET:
			return set(keyValue);
		case STREAM:
			return stream(keyValue);
		case STRING:
			return string(keyValue);
		case TIMESERIES:
			return timeseries(keyValue);
		case ZSET:
			return zset(keyValue);
		default:
			return keyValue.getValue();
		}
	}

	@SuppressWarnings("unchecked")
	private Collection<V> members(MemKeyValue<K, Object> keyValue) {
		return (Collection<V>) keyValue.getValue();
	}

	@SuppressWarnings("unchecked")
	private V string(MemKeyValue<K, Object> keyValue) {
		return (V) keyValue.getValue();
	}

	@SuppressWarnings("unchecked")
	private Collection<Sample> timeseries(MemKeyValue<K, Object> keyValue) {
		List<List<Object>> value = (List<List<Object>>) keyValue.getValue();
		List<Sample> sampleList = new ArrayList<>();
		for (List<Object> sample : value) {
			LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
			Long timestamp = (Long) sample.get(0);
			sampleList.add(Sample.of(timestamp, toDouble(sample.get(1))));
		}
		return sampleList;
	}

	private double toDouble(Object value) {
		return Double.parseDouble(toString(value));
	}

	private Map<K, V> hash(MemKeyValue<K, Object> keyValue) {
		return map(keyValue.getValue());
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> map(Object value) {
		List<Object> list = (List<Object>) value;
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i < list.size(); i += 2) {
			map.put((K) list.get(i), (V) list.get(i + 1));
		}
		return map;
	}

	private Set<V> set(MemKeyValue<K, Object> keyValue) {
		return new HashSet<>(members(keyValue));
	}

	@SuppressWarnings("unchecked")
	private Set<ScoredValue<V>> zset(MemKeyValue<K, Object> keyValue) {
		List<Object> list = (List<Object>) keyValue.getValue();
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Set<ScoredValue<V>> values = new HashSet<>();
		for (int i = 0; i < list.size(); i += 2) {
			double score = toDouble(list.get(i + 1));
			values.add(ScoredValue.just(score, (V) list.get(i)));
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private Collection<StreamMessage<K, V>> stream(MemKeyValue<K, Object> keyValue) {
		List<List<Object>> value = (List<List<Object>>) keyValue.getValue();
		List<StreamMessage<K, V>> messages = new ArrayList<>();
		for (List<Object> message : value) {
			LettuceAssert.isTrue(message.size() == 2, "Invalid list size: " + message.size());
			String id = toString(message.get(0));
			Map<K, V> body = map(message.get(1));
			messages.add(new StreamMessage<>(keyValue.getKey(), id, body));
		}
		return messages;
	}

}
