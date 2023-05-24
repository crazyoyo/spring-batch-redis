package com.redis.spring.batch.writer;

import java.time.Duration;

public class ReplicaWaitOptions {

	public static final int DEFAULT_REPLICAS = 1;
	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

	private final int replicas;
	private final Duration timeout;

	private ReplicaWaitOptions(int replicas, Duration timeout) {
		this.replicas = replicas;
		this.timeout = timeout;
	}

	public int getReplicas() {
		return replicas;
	}

	public Duration getTimeout() {
		return timeout;
	}

	public static ReplicaWaitOptions of(int replicas, Duration timeout) {
		return new ReplicaWaitOptions(replicas, timeout);
	}

	public static ReplicaWaitOptions of(int replicas) {
		return new ReplicaWaitOptions(replicas, DEFAULT_TIMEOUT);
	}

	public static ReplicaWaitOptions of(Duration timeout) {
		return new ReplicaWaitOptions(DEFAULT_REPLICAS, timeout);
	}

}