package com.redis.spring.batch.gen;

import java.time.Instant;

public class TimeSeriesOptions {

	public static final Range DEFAULT_SAMPLE_COUNT = Range.of(10);

	private Range sampleCount = DEFAULT_SAMPLE_COUNT;
	private Instant startTime = Instant.now();

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
