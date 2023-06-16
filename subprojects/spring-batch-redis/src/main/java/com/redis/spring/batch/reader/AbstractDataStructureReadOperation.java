package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructure;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.internal.LettuceAssert;

public abstract class AbstractDataStructureReadOperation<K, V>
		extends AbstractLuaReadOperation<K, V, DataStructure<K>> {

	protected AbstractDataStructureReadOperation(AbstractRedisClient client) {
		super(client, "datastructure.lua");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DataStructure<K> convert(List<Object> list) {
		if (list == null) {
			return null;
		}
		DataStructure<K> ds = new DataStructure<>();
		Iterator<Object> iterator = list.iterator();
		if (iterator.hasNext()) {
			ds.setKey((K) iterator.next());
		}
		if (iterator.hasNext()) {
			ds.setTtl((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			ds.setType(string(iterator.next()));
		}
		if (iterator.hasNext()) {
			ds.setValue(value(ds, iterator.next()));
		}
		return ds;
	}

	@SuppressWarnings("unchecked")
	private Object value(DataStructure<K> ds, Object object) {
		switch (ds.getType()) {
		case DataStructure.HASH:
			return map((List<Object>) object);
		case DataStructure.ZSET:
			return zset((List<Object>) object);
		case DataStructure.STREAM:
			return stream(ds.getKey(), (List<Object>) object);
		case DataStructure.TIMESERIES:
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
	private List<ScoredValue<V>> zset(List<Object> list) {
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		List<ScoredValue<V>> values = new ArrayList<>();
		for (int i = 0; i < list.size(); i += 2) {
			values.add(ScoredValue.just(Double.parseDouble(string(list.get(i + 1))), (V) list.get(i)));
		}
		return values;
	}

	protected abstract String string(Object object);

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
