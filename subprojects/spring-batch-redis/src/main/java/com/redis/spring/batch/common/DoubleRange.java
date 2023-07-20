package com.redis.spring.batch.common;

import java.util.Objects;

public class DoubleRange {

    public static final String SEPARATOR = ":";

    private final double min;

    private final double max;

    private DoubleRange(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public boolean contains(double value) {
        return value >= min && value <= max;
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
        DoubleRange other = (DoubleRange) obj;
        return Double.doubleToLongBits(max) == Double.doubleToLongBits(other.max)
                && Double.doubleToLongBits(min) == Double.doubleToLongBits(other.min);
    }

    @Override
    public String toString() {
        if (min == max) {
            return String.valueOf(min);
        }
        return min + SEPARATOR + max;
    }

    public static DoubleRange is(double value) {
        return new DoubleRange(value, value);
    }

    public static DoubleRange between(double min, double max) {
        return new DoubleRange(min, max);
    }

}
