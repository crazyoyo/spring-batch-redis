package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.LongRange;

public class MapOptions {

    public static final LongRange DEFAULT_FIELD_COUNT = LongRange.is(10);

    public static final LongRange DEFAULT_FIELD_LENGTH = LongRange.is(100);

    private LongRange fieldCount = DEFAULT_FIELD_COUNT;

    private LongRange fieldLength = DEFAULT_FIELD_LENGTH;

    public LongRange getFieldCount() {
        return fieldCount;
    }

    public void setFieldCount(LongRange count) {
        this.fieldCount = count;
    }

    public LongRange getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(LongRange length) {
        this.fieldLength = length;
    }

}
