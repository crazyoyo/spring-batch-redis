package com.redis.spring.batch.reader;

import java.util.concurrent.atomic.AtomicLong;

public class KeyContext<K> {

	private final AtomicLong count = new AtomicLong();
	private K key;
	private String type;
	private long memoryUsage;
	private long startTime = System.currentTimeMillis();
	private long endTime;

	public KeyContext() {
	}

	public KeyContext(K key) {
		this.key = key;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getMemoryUsage() {
		return memoryUsage;
	}

	public void setMemoryUsage(long memoryUsage) {
		this.memoryUsage = memoryUsage;
	}

	public long getEndTime() {
		return endTime;
	}

	public long incrementCount() {
		endTime = System.currentTimeMillis();
		return count.incrementAndGet();
	}

	public long getDuration() {
		return endTime - startTime;
	}

	public void reset() {
		this.startTime = System.currentTimeMillis();
		this.endTime = this.startTime;
		this.count.set(0);
	}

}