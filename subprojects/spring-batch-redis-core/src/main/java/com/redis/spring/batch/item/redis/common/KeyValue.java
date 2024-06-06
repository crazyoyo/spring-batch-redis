package com.redis.spring.batch.item.redis.common;

import org.springframework.util.StringUtils;

public class KeyValue<K, T> {

	public static final long TTL_NONE = -1;
	public static final long TTL_NO_KEY = -2;

	private K key;
	private String type;
	private T value;
	private long time;
	private long ttl;
	private long memoryUsage;

	public KeyValue() {
	}

	public KeyValue(KeyValue<K, T> other) {
		this.key = other.key;
		this.ttl = other.ttl;
		this.type = other.type;
		this.value = other.value;
		this.memoryUsage = other.memoryUsage;
		this.time = other.time;
	}

	public static boolean exists(KeyValue<?, ?> keyValue) {
		return keyValue != null && hasKey(keyValue) && keyValue.getTtl() != TTL_NO_KEY
				&& type(keyValue) != DataType.NONE;
	}

	public static boolean hasKey(KeyValue<?, ?> keyValue) {
		return keyValue.key != null;
	}

	public static boolean hasTtl(KeyValue<?, ?> keyValue) {
		return keyValue.getTtl() > 0;
	}

	public static boolean hasValue(KeyValue<?, ?> keyValue) {
		return keyValue.getValue() != null;
	}

	public static boolean hasType(KeyValue<?, ?> keyValue) {
		return StringUtils.hasLength(keyValue.getType());
	}

	public static DataType type(KeyValue<?, ?> keyValue) {
		if (hasType(keyValue)) {
			return DataType.of(keyValue.getType());
		}
		return null;
	}

	/**
	 * 
	 * @param keyValue the KeyValue to get expiration time from
	 * @return Expiration time of a Redis key or -1 if no expire time is set for
	 *         this key
	 */
	public static long absoluteTTL(KeyValue<?, ?> keyValue) {
		if (hasTtl(keyValue)) {
			return keyValue.getTime() + keyValue.getTtl();
		}
		return TTL_NONE;
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

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	/**
	 * 
	 * @return number of bytes that a Redis key and its value require to be stored
	 *         in RAM
	 */
	public long getMemoryUsage() {
		return memoryUsage;
	}

	public void setMemoryUsage(long mem) {
		this.memoryUsage = mem;
	}

	/**
	 * 
	 * @return remaining time to live in milliseconds of a Redis key that has an
	 *         expire set
	 */
	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	/**
	 * 
	 * @return POSIX time in milliseconds when the key was read from Redis
	 */
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + "]";
	}

}
