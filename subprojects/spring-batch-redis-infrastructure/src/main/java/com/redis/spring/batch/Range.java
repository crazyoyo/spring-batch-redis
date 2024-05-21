package com.redis.spring.batch;

import java.util.Objects;

import org.springframework.util.StringUtils;

public class Range {

	public static final String SEPARATOR = ":";
	private static final String UNBOUNDED = "*";

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

	@Override
	public String toString() {
		if (min == max) {
			return String.valueOf(min);
		}
		return toString(min) + SEPARATOR + toString(max);
	}

	private static String toString(int value) {
		if (value == Integer.MAX_VALUE || value == Integer.MIN_VALUE) {
			return UNBOUNDED;
		}
		return String.valueOf(value);
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

	public static Range parse(String value) {
		return of(value, SEPARATOR);
	}

	private static Range of(String string, String separator) {
		try {
			int pos = string.indexOf(separator);
			if (pos == -1) {
				int value = Integer.parseInt(string);
				return Range.of(value);
			}
			int min = min(string.substring(0, pos).trim());
			int max = max(string.substring(pos + 1).trim());
			return Range.of(min, max);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid range. Range should be in the form 'int:int'", e);
		}

	}

	private static int min(String string) {
		if (StringUtils.hasLength(string)) {
			return Integer.parseInt(string);
		}
		return 0;
	}

	private static int max(String string) {
		if (StringUtils.hasLength(string) && !string.equals(UNBOUNDED)) {
			return Integer.parseInt(string);
		}
		return Integer.MAX_VALUE;
	}

}
