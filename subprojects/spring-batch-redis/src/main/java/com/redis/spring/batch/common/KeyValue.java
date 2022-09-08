package com.redis.spring.batch.common;

import java.util.Objects;

public class KeyValue<K> {

	/**
	 * Redis key.
	 *
	 */
	private K key;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyValue<?> other = (KeyValue<?>) obj;
		return Objects.equals(key, other.key);
	}

}
