package com.redis.spring.batch.gen;

import com.redis.spring.batch.common.Range;

public class StringOptions {

    public static final Range DEFAULT_LENGTH = Range.of(100);

    private Range length = DEFAULT_LENGTH;

    public Range getLength() {
        return length;
    }

    public void setLength(Range length) {
        this.length = length;
    }

}
