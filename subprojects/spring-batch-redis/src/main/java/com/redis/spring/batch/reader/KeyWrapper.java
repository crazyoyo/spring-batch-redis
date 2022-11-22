package com.redis.spring.batch.reader;

import java.util.Arrays;

public class KeyWrapper<K> {

	protected final K key;

	public KeyWrapper(K name) {
		this.key = name;
	}

	public K getKey() {
		return key;
	}

	@Override
	public int hashCode() {

		if (key instanceof byte[]) {
			return Arrays.hashCode((byte[]) key);
		}
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof KeyWrapper)) {
			return false;
		}

		KeyWrapper<?> that = (KeyWrapper<?>) obj;

		if (key instanceof byte[] && that.key instanceof byte[]) {
			return Arrays.equals((byte[]) key, (byte[]) that.key);
		}

		return key.equals(that.key);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(getClass().getSimpleName());
		sb.append(" [key=").append(key);
		sb.append(']');
		return sb.toString();
	}

}