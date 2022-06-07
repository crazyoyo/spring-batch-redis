package com.redis.spring.batch;

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
	private Long ttl;

	public boolean hasTtl() {
		return ttl != null && ttl > 0;
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

	public Long getTtl() {
		return ttl;
	}

	public void setTtl(long absoluteTtl) {
		this.ttl = absoluteTtl;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", value=" + value + ", ttl=" + ttl + "]";
	}

}
