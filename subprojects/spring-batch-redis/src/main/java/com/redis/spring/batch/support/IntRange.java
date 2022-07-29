package com.redis.spring.batch.support;

import java.util.Objects;

public class IntRange {

	public static final String SEPARATOR = ":";

	private final int min;
	private final int max;

	private IntRange(int min, int max) {
		this.min = min;
		this.max = max;
	}

	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}

	@Override
	public int hashCode() {
		return Objects.hash(max, min);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntRange other = (IntRange) obj;
		return max == other.max && min == other.min;
	}

	@Override
	public String toString() {
		if (min == max) {
			return String.valueOf(min);
		}
		return min + SEPARATOR + max;
	}

	public static IntRange is(int value) {
		return new IntRange(value, value);
	}

	public static IntRange between(int min, int max) {
		return new IntRange(min, max);
	}

}
