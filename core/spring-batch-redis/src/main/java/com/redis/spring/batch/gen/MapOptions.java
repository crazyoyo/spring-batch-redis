package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.IntRange;

public class MapOptions {

    public static final IntRange DEFAULT_FIELD_COUNT = IntRange.is(10);

    public static final IntRange DEFAULT_FIELD_LENGTH = IntRange.is(100);

    private IntRange fieldCount = DEFAULT_FIELD_COUNT;

    private IntRange fieldLength = DEFAULT_FIELD_LENGTH;

    public IntRange getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(IntRange count) {
        this.fieldCount = count;
    }

    public IntRange getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(IntRange length) {
        this.fieldLength = length;
    }

}
