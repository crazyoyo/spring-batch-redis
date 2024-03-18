package com.redis.spring.batch.common;

import java.util.Objects;

public class KeyValue<K> {

	public static final long TTL_KEY_DOES_NOT_EXIST = -2;

	private K key;

	private DataType type;

	private Object value;

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private long ttl;

	/**
	 * Number of bytes that this key and its value require to be stored in Redis
	 * RAM. 0 means no memory usage information is available.
	 */
	private long memoryUsage;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public long getMemoryUsage() {
		return memoryUsage;
	}

	public void setMemoryUsage(long memoryUsage) {
		this.memoryUsage = memoryUsage;
	}

	public boolean exists() {
		return type != null && ttl != KeyValue.TTL_KEY_DOES_NOT_EXIST;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, memoryUsage, ttl, type, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyValue<?> other = (KeyValue<?>) obj;
		return Objects.equals(key, other.key) && memoryUsage == other.memoryUsage && ttl == other.ttl
				&& type == other.type && Objects.equals(value, other.value);
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", type=" + type + ", value=" + value + ", ttl=" + ttl + ", memoryUsage="
				+ memoryUsage + "]";
	}

}
