package com.redis.spring.batch.reader;

import java.util.Objects;

import com.redis.spring.batch.KeyValue.Type;

public class KeyEvent<K> {

	private final Wrapper<K> key;
	private final Type type;

	public KeyEvent(K key, Type type) {
		this.key = new Wrapper<>(key);
		this.type = type;
	}

	public K getKey() {
		return key.getValue();
	}

	public Type getType() {
		return type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyEvent other = (KeyEvent) obj;
		return Objects.equals(key, other.key);
	}

}
