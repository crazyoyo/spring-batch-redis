package com.redis.spring.batch.support;

public class KeyValue<K, T> {

	/**
	 * Redis key.
	 *
	 */
	private K key;

	/**
	 * Redis value. Null if key does not exist
	 * 
	 */
	private T value;

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private Long absoluteTTL;

	public KeyValue() {
	}

	public KeyValue(K key) {
		this.key = key;
	}

	public KeyValue(K key, T value) {
		this.key = key;
		this.value = value;
	}

	public KeyValue(K key, T value, Long absoluteTTL) {
		this.key = key;
		this.value = value;
		this.absoluteTTL = absoluteTTL;
	}

	public boolean hasTTL() {
		return absoluteTTL != null && absoluteTTL > 0;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public Long getAbsoluteTTL() {
		return absoluteTTL;
	}

	public void setAbsoluteTTL(Long absoluteTTL) {
		this.absoluteTTL = absoluteTTL;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", value=" + value + ", absoluteTTL=" + absoluteTTL + "]";
	}

}
