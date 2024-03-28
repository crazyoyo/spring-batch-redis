package com.redis.spring.batch.gen;

public class MapOptions {

    public static final Range DEFAULT_FIELD_COUNT = Range.of(10);

    public static final Range DEFAULT_FIELD_LENGTH = Range.of(100);

    private Range fieldCount = DEFAULT_FIELD_COUNT;

    private Range fieldLength = DEFAULT_FIELD_LENGTH;

    public Range getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(Range count) {
        this.fieldCount = count;
    }

    public Range getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(Range length) {
        this.fieldLength = length;
    }

}
