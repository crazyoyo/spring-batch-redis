package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.DoubleRange;

public class ZsetOptions extends CollectionOptions {

    public static final DoubleRange DEFAULT_SCORE = DoubleRange.between(0, 100);

    private DoubleRange score = DEFAULT_SCORE;

    public DoubleRange getScore() {
        return score;
    }

    public void setScore(DoubleRange score) {
        this.score = score;
    }

}
