package com.redis.spring.batch.gen;

import java.time.Instant;

import com.redis.spring.batch.util.IntRange;

public class TimeSeriesOptions {

    public static final IntRange DEFAULT_SAMPLE_COUNT = IntRange.is(10);

    private IntRange sampleCount = DEFAULT_SAMPLE_COUNT;

    private Instant startTime;

    public IntRange getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(IntRange sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

}
