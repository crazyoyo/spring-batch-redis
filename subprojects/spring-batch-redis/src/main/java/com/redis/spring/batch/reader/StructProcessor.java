package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class StructProcessor<K, V> implements ItemProcessor<KeyValue<K>, KeyValue<K>> {

	private final Function<V, String> toStringValueFunction;

	public StructProcessor(RedisCodec<K, V> codec) {
		this.toStringValueFunction = Utils.toStringValueFunction(codec);
	}

	@Override
	public KeyValue<K> process(KeyValue<K> item) throws Exception {
		if (item.getValue() != null) {
			switch (item.getType()) {
			case KeyValue.HASH:
				item.setValue(map(item.getValue()));
				break;
			case KeyValue.SET:
				item.setValue(new HashSet<>(item.getValue()));
				break;
			case KeyValue.ZSET:
				item.setValue(zset(item.getValue()));
				break;
			case KeyValue.STREAM:
				item.setValue(stream(item.getKey(), item.getValue()));
				break;
			case KeyValue.TIMESERIES:
				item.setValue(timeSeries(item.getValue()));
				break;
			default:
				// do nothing
				break;
			}
		}
		return item;
	}

	@SuppressWarnings("unchecked")
	private List<Sample> timeSeries(List<Object> list) {
		List<Sample> samples = new ArrayList<>();
		for (Object entry : list) {
			List<Object> sample = (List<Object>) entry;
			LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
			samples.add(Sample.of((Long) sample.get(0), toDouble((V) sample.get(1))));
		}
		return samples;
	}

	private double toDouble(V value) {
		return Double.parseDouble(toStringValueFunction.apply(value));
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
			values.add(ScoredValue.just(toDouble((V) list.get(i + 1)), (V) list.get(i)));
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private List<StreamMessage<K, V>> stream(K key, List<Object> list) {
		List<StreamMessage<K, V>> messages = new ArrayList<>();
		for (Object object : list) {
			List<Object> entry = (List<Object>) object;
			LettuceAssert.isTrue(entry.size() == 2, "Invalid list size: " + entry.size());
			messages.add(
					new StreamMessage<>(key, toStringValueFunction.apply((V) entry.get(0)), map((List<Object>) entry.get(1))));
		}
		return messages;
	}

}
