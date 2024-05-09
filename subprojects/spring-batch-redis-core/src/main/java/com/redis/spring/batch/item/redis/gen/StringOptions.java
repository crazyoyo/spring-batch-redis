package com.redis.spring.batch.item.redis.gen;

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
