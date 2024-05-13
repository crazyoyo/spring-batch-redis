package com.redis.spring.batch.item.redis.gen;

import com.redis.spring.batch.Range;

public class ZsetOptions extends CollectionOptions {

    public static final Range DEFAULT_SCORE = Range.of(0, 100);

    private Range score = DEFAULT_SCORE;

    public Range getScore() {
        return score;
    }

    public void setScore(Range score) {
        this.score = score;
    }

}
