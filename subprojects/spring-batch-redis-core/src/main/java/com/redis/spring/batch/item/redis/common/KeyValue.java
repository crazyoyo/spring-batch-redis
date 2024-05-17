package com.redis.spring.batch.item.redis.common;

import org.springframework.util.StringUtils;

public class KeyValue<K, T> {

	public static final long TTL_NO_KEY = -2;

	private K key;
	private String type;
	private T value;
	private long ttl;

	public KeyValue() {
	}

	public KeyValue(KeyValue<K, T> other) {
		this.key = other.key;
		this.ttl = other.ttl;
		this.type = other.type;
		this.value = other.value;
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
	 * @return Expiration POSIX time in milliseconds
	 */
	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public static boolean exists(KeyValue<?, ?> kv) {
		return kv != null && hasKey(kv) && kv.getTtl() != TTL_NO_KEY && type(kv) != DataType.NONE;
	}

	public static boolean hasKey(KeyValue<?, ?> kv) {
		return kv.getKey() != null;
	}

	public static boolean hasTtl(KeyValue<?, ?> kv) {
		return kv.getTtl() > 0;
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

}
