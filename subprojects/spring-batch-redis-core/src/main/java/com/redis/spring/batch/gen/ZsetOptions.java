package com.redis.spring.batch.gen;

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
