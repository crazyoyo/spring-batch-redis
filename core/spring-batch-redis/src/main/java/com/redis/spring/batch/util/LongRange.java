package com.redis.spring.batch.util;

import java.util.Objects;
import java.util.function.LongPredicate;

public class LongRange {

    public static final String SEPARATOR = ":";

    private static final LongRange UNBOUNDED = new LongRange(Long.MIN_VALUE, Long.MAX_VALUE);

    private final long min;

    private final long max;

    private LongRange(long min, long max) {
        this.min = min;
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public boolean contains(long value) {
        return value >= min && value <= max;
    }

    public LongPredicate asPredicate() {
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
        LongRange other = (LongRange) obj;
        return max == other.max && min == other.min;
    }

    @Override
    public String toString() {
        if (min == max) {
            return String.valueOf(min);
        }
        return min + SEPARATOR + max;
    }

    public static LongRange is(long value) {
        return new LongRange(value, value);
    }

    public static LongRange between(long min, long max) {
        return new LongRange(min, max);
    }

    public static LongRange from(long min) {
        return new LongRange(min, Long.MAX_VALUE);
    }

    public static LongRange to(long max) {
        return new LongRange(0, max);
    }

    public static LongRange unbounded() {
        return UNBOUNDED;
    }

}
