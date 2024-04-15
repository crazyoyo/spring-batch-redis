package com.redis.spring.batch.gen;

import java.util.Objects;

public class Range {

	private final int min;
	private final int max;

	public Range(int min, int max) {
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
		Range other = (Range) obj;
		return max == other.max && min == other.min;
	}

	public static Range of(int value) {
		return new Range(value, value);
	}

	public static Range of(int min, int max) {
		return new Range(min, max);
	}

	public static Range from(int min) {
		return new Range(min, Integer.MAX_VALUE);
	}

	public static Range to(int max) {
		return new Range(0, max);
	}

	public static Range unbounded() {
		return new Range(0, Integer.MAX_VALUE);
	}

}
