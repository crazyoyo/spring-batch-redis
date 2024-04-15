package com.redis.spring.batch.reader;

import java.util.Arrays;

import com.redis.spring.batch.KeyValue.Type;

public class KeyEvent<K> {

	private final K key;
	private final Type type;

	public KeyEvent(K key, Type type) {
		this.key = key;
		this.type = type;
	}

	public K getKey() {
		return key;
	}

	public Type getType() {
		return type;
	}

	@Override
	public int hashCode() {
		if (key instanceof byte[]) {
			return Arrays.hashCode((byte[]) key);
		}
		return key.hashCode();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof KeyEvent)) {
			return false;
		}

		KeyEvent<K> that = (KeyEvent<K>) obj;

		if (key instanceof byte[] && that.key instanceof byte[]) {
			return Arrays.equals((byte[]) key, (byte[]) that.key);
		}

		return key.equals(that.key);
	}

}
