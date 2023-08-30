package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.IntRange;

public class StringOptions {

    public static final IntRange DEFAULT_LENGTH = IntRange.is(100);

    private IntRange length = DEFAULT_LENGTH;

    public IntRange getLength() {
        return length;
    }

    public void setLength(IntRange length) {
        this.length = length;
    }

}
