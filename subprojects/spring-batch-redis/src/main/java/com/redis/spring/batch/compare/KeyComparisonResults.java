package com.redis.spring.batch.compare;

import java.util.concurrent.atomic.AtomicLong;

public class KeyComparisonResults {

	private final AtomicLong source = new AtomicLong();
	private final AtomicLong ok = new AtomicLong();
	private final AtomicLong missing = new AtomicLong();
	private final AtomicLong type = new AtomicLong();
	private final AtomicLong ttl = new AtomicLong();
	private final AtomicLong value = new AtomicLong();

	/**
	 * Number of keys in source database
	 */
	public long getSource() {
		return source.get();
	}

	/**
	 * Number of identical keys
	 */
	public long getOK() {
		return ok.get();
	}

	/**
	 * Number of keys missing in target database
	 */
	public long getMissing() {
		return missing.get();
	}

	/**
	 * Number of keys with mismatched TTL
	 */
	public long getTTL() {
		return ttl.get();
	}

	/**
	 * Number of keys with mismatched type (e.g. left=HASH vs right=STRING)
	 */
	public long getType() {
		return type.get();
	}

	/**
	 * Number of keys with mismatched values (e.g. "ABC" vs "ABD")
	 */
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

	/**
	 * 
	 * @return Number of keys in target database
	 */
	public long getTarget() {
		return getOK() + getTTL() + getType() + getValue();
	}

}
