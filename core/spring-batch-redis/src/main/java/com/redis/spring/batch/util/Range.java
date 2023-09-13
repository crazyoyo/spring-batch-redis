package com.redis.spring.batch.util;

import java.util.Objects;
import java.util.function.IntPredicate;

import org.springframework.util.StringUtils;

public class Range {

    public static final String DEFAULT_SEPARATOR = ":";

    public static final int MIN_VALUE = 0;

    public static final int MAX_VALUE = Integer.MAX_VALUE;

    private static final Range UNBOUNDED = new Range(MIN_VALUE, MAX_VALUE);

    private static final String VARIABLE = "*";

    private final int min;

    private final int max;

    private String separator = DEFAULT_SEPARATOR;

    private Range(int min, int max) {
        this.min = min;
        this.max = max;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public boolean contains(int value) {
        return value >= min && value <= max;
    }

    public IntPredicate asPredicate() {
        return this::contains;
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

    @Override
    public String toString() {
        if (min == max) {
            return toString(min);
        }
        return min + separator + max;
    }

    private String toString(int value) {
        if (value == MAX_VALUE) {
            return VARIABLE;
        }
        return String.valueOf(value);
    }

    public static Range of(String string, String separator) {
        try {
            int pos = string.indexOf(separator);
            if (pos == -1) {
                return of(Integer.parseInt(string));
            }
            int min = min(string.substring(0, pos).trim());
            int max = max(string.substring(pos + 1).trim());
            return of(min, max);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid range. Range should be in the form 'int:int'", e);
        }

    }

    public static Range of(String string) {
        return of(string, DEFAULT_SEPARATOR);
    }

    private static int min(String string) {
        if (StringUtils.hasLength(string)) {
            return Integer.parseInt(string);
        }
        return MIN_VALUE;
    }

    private static int max(String string) {
        if (StringUtils.hasLength(string) && !string.equals(VARIABLE)) {
            return Integer.parseInt(string);
        }
        return MAX_VALUE;
    }

    public static Range of(int value) {
        return new Range(value, value);
    }

    public static Range of(int min, int max) {
        return new Range(min, max);
    }

    public static Range from(int min) {
        return new Range(min, MAX_VALUE);
    }

    public static Range to(int max) {
        return new Range(MIN_VALUE, max);
    }

    public static Range unbounded() {
        return UNBOUNDED;
    }

}
