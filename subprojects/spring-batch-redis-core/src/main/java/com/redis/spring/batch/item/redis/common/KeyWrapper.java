package com.redis.spring.batch.item.redis.common;

import java.util.Arrays;

public class KeyWrapper<K> {

	private final K key;

	public KeyWrapper(K key) {
		this.key = key;
	}

	public K getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		return hashCode(key);
	}

	public static <K> int hashCode(K key) {
		if (key instanceof byte[]) {
			return Arrays.hashCode((byte[]) key);
		}
		return key.hashCode();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof KeyWrapper)) {
			return false;
		}

		KeyWrapper<K> that = (KeyWrapper<K>) obj;

		return equals(this.key, that.key);
	}

	public static <K> boolean equals(K key, K other) {
		if (key instanceof byte[] && other instanceof byte[]) {
			return Arrays.equals((byte[]) key, (byte[]) other);
		}
		return key.equals(other);
	}

}
