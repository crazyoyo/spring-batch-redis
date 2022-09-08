package com.redis.spring.batch.writer;

import java.time.Duration;

public class WaitForReplication {

	public static final int DEFAULT_REPLICAS = 1;
	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

	private final int replicas;
	private final Duration timeout;

	private WaitForReplication(int replicas, Duration timeout) {
		this.replicas = replicas;
		this.timeout = timeout;
	}

	public int getReplicas() {
		return replicas;
	}

	public Duration getTimeout() {
		return timeout;
	}

	public static WaitForReplication of(int replicas, Duration timeout) {
		return new WaitForReplication(replicas, timeout);
	}

	public static WaitForReplication of(int replicas) {
		return new WaitForReplication(replicas, DEFAULT_TIMEOUT);
	}

	public static WaitForReplication of(Duration timeout) {
		return new WaitForReplication(DEFAULT_REPLICAS, timeout);
	}

}