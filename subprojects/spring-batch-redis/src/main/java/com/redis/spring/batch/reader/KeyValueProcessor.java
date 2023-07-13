package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyValueProcessor<K, V> implements ItemProcessor<List<Object>, KeyValue<K>> {

	private final RedisCodec<K, V> codec;

	public KeyValueProcessor(RedisCodec<K, V> codec) {
		this.codec = codec;
	}

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<K> process(List<Object> item) throws Exception {
		if (item == null) {
			return null;
		}
		KeyValue<K> keyValue = new KeyValue<>();
		Iterator<Object> iterator = item.iterator();
		if (iterator.hasNext()) {
			keyValue.setKey((K) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setType(decodeValue((V) iterator.next()));
		}
		if (iterator.hasNext()) {
			keyValue.setTtl((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setMemoryUsage((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setValue(iterator.next());
		}
		return keyValue;
	}

	private String decodeValue(V value) {
		if (codec instanceof StringCodec) {
			return (String) value;
		}
		return StringCodec.UTF8.decodeValue(codec.encodeValue(value));
	}

}
