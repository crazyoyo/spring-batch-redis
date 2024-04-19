package com.redis.spring.batch.reader;

import java.time.Duration;

public class KeyComparatorOptions {

	public enum StreamMessageIdPolicy {
		COMPARE, IGNORE
	}

	public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);
	public static final StreamMessageIdPolicy DEFAULT_STREAM_MESSAGE_ID_POLICY = StreamMessageIdPolicy.COMPARE;

	private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
	private StreamMessageIdPolicy streamMessageIdPolicy = DEFAULT_STREAM_MESSAGE_ID_POLICY;

	public Duration getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public StreamMessageIdPolicy getStreamMessageIdPolicy() {
		return streamMessageIdPolicy;
	}

	public void setStreamMessageIdPolicy(StreamMessageIdPolicy streamMessageIdPolicy) {
		this.streamMessageIdPolicy = streamMessageIdPolicy;
	}

}
