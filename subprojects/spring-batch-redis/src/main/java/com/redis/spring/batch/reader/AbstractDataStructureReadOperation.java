package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.internal.LettuceAssert;

public abstract class AbstractDataStructureReadOperation<K, V>
		extends AbstractLuaReadOperation<K, V, DataStructure<K>> {

	private static final String FILENAME = "datastructure.lua";

	protected AbstractDataStructureReadOperation(AbstractRedisClient client) {
		super(client, FILENAME);
	}

	@Override
	protected DataStructure<K> keyValue() {
		return new DataStructure<>();
	}

	@Override
	protected void setValue(DataStructure<K> keyValue, Object value) {
		keyValue.setValue(value(keyValue, value));
	}

	@SuppressWarnings("unchecked")
	private Object value(DataStructure<K> ds, Object object) {
		switch (ds.getType()) {
		case KeyValue.HASH:
			return map((List<Object>) object);
		case KeyValue.SET:
			return new HashSet<>((Collection<Object>) object);
		case KeyValue.ZSET:
			return zset((List<Object>) object);
		case KeyValue.STREAM:
			return stream(ds.getKey(), (List<Object>) object);
		case KeyValue.TIMESERIES:
			return timeSeries((List<Object>) object);
		default:
			return object;
		}
	}

	@SuppressWarnings("unchecked")
	private List<Sample> timeSeries(List<Object> list) {
		List<Sample> samples = new ArrayList<>();
		for (Object entry : list) {
			List<Object> sample = (List<Object>) entry;
			LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
			samples.add(Sample.of((Long) sample.get(0), Double.parseDouble(string(sample.get(1)))));
		}
		return samples;
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
	private Set<ScoredValue<V>> zset(List<Object> list) {
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Set<ScoredValue<V>> values = new HashSet<>();
		for (int i = 0; i < list.size(); i += 2) {
			values.add(ScoredValue.just(Double.parseDouble(string(list.get(i + 1))), (V) list.get(i)));
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private List<StreamMessage<K, V>> stream(K key, List<Object> list) {
		List<StreamMessage<K, V>> messages = new ArrayList<>();
		for (Object object : list) {
			List<Object> entry = (List<Object>) object;
			LettuceAssert.isTrue(entry.size() == 2, "Invalid list size: " + entry.size());
			messages.add(new StreamMessage<>(key, string(entry.get(0)), map((List<Object>) entry.get(1))));
		}
		return messages;
	}

}
