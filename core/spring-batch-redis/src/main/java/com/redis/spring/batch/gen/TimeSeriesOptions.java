package com.redis.spring.batch.gen;

import java.time.Instant;

import com.redis.spring.batch.util.Range;

public class TimeSeriesOptions {

    public static final Range DEFAULT_SAMPLE_COUNT = Range.of(10);

    private Range sampleCount = DEFAULT_SAMPLE_COUNT;

    private Instant startTime;

    public Range getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(Range sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

}
