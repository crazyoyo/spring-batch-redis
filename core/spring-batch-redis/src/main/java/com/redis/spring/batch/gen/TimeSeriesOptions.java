package com.redis.spring.batch.gen;

import java.time.Instant;

import com.redis.spring.batch.util.LongRange;

public class TimeSeriesOptions {

    public static final LongRange DEFAULT_SAMPLE_COUNT = LongRange.is(10);

    private LongRange sampleCount = DEFAULT_SAMPLE_COUNT;

    private Instant startTime;

    public LongRange getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(LongRange sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

}
