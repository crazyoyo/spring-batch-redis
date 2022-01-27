package com.redis.spring.batch.compare;

import java.util.concurrent.atomic.AtomicLong;

public class KeyComparisonResults {

	/**
	 * Number of keys in source database
	 */
	private final AtomicLong source = new AtomicLong();
	/**
	 * Number of identical keys
	 */
	private final AtomicLong ok = new AtomicLong();
	/**
	 * Number of keys missing in target database
	 */
	private final AtomicLong missing = new AtomicLong();
	/**
	 * Number of keys with mismatched type (e.g. left=HASH vs right=STRING)
	 */
	private final AtomicLong type = new AtomicLong();
	/**
	 * Number of keys with mismatched TTL
	 */
	private final AtomicLong ttl = new AtomicLong();
	/**
	 * Number of keys with mismatched values (e.g. "ABC" vs "ABD")
	 */
	private final AtomicLong value = new AtomicLong();

	public long getSource() {
		return source.get();
	}

	public long getOK() {
		return ok.get();
	}

	public long getMissing() {
		return missing.get();
	}

	public long getTTL() {
		return ttl.get();
	}

	public long getType() {
		return type.get();
	}

	public long getValue() {
		return value.get();
	}

	public long incrementOK() {
		return ok.incrementAndGet();
	}

	public long incrementMissing() {
		return missing.incrementAndGet();
	}

	public long incrementTTL() {
		return ttl.incrementAndGet();
	}

	public long incrementType() {
		return type.incrementAndGet();
	}

	public long incrementValue() {
		return value.incrementAndGet();
	}

	public long incrementSource() {
		return source.incrementAndGet();
	}

	public long addAndGetSource(long delta) {
		return source.addAndGet(delta);
	}

	public boolean isOK() {
		return source.get() == ok.get();
	}

}
