package com.redis.spring.batch.item.redis.reader;

import java.util.Arrays;

public class Wrapper<T> {

	private final T value;

	public Wrapper(T key) {
		this.value = key;
	}

	public T getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		if (value instanceof byte[]) {
			return Arrays.hashCode((byte[]) value);
		}
		return value.hashCode();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof Wrapper)) {
			return false;
		}

		Wrapper<T> that = (Wrapper<T>) obj;

		if (value instanceof byte[] && that.value instanceof byte[]) {
			return Arrays.equals((byte[]) value, (byte[]) that.value);
		}

		return value.equals(that.value);
	}

}
