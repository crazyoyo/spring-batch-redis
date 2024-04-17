package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.codec.RedisCodec;

public class EvalFunction<K, V> implements Function<List<Object>, KeyValue<K>> {

	private final Function<V, String> toStringValueFunction;

	public EvalFunction(RedisCodec<K, V> codec) {
		this.toStringValueFunction = BatchUtils.toStringValueFunction(codec);
	}

	@SuppressWarnings("unchecked")
	@Override
	public KeyValue<K> apply(List<Object> list) {
		Iterator<Object> iterator = list.iterator();
		KeyValue<K> keyValue = new KeyValue<>();
		if (iterator.hasNext()) {
			keyValue.setKey((K) iterator.next());
			if (iterator.hasNext()) {
				keyValue.setTtl((Long) iterator.next());
				if (iterator.hasNext()) {
					keyValue.setType(KeyValue.Type.of(toString(iterator.next())));
					if (iterator.hasNext()) {
						keyValue.setMem((Long) iterator.next());
						if (iterator.hasNext()) {
							keyValue.setValue(iterator.next());
						}
					}
				}
			}
		}
		return keyValue;
	}

	@SuppressWarnings("unchecked")
	protected String toString(Object value) {
		return toStringValueFunction.apply((V) value);
	}

}
