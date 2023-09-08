package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.LongRange;

public class StringOptions {

    public static final LongRange DEFAULT_LENGTH = LongRange.is(100);

    private LongRange length = DEFAULT_LENGTH;

    public LongRange getLength() {
        return length;
    }

    public void setLength(LongRange length) {
        this.length = length;
    }

}
