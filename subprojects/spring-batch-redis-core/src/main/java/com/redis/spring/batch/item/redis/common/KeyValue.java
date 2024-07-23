package com.redis.spring.batch.item.redis.common;

import org.springframework.util.StringUtils;

public class KeyValue<K, T> extends KeyEvent<K> {

	public static final long TTL_NONE = -1;
	public static final long TTL_NO_KEY = -2;

	private String type;
	private long ttl;
	private T value;
	private long memoryUsage;

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

	public void setMemoryUsage(long memUsage) {
		this.memoryUsage = memUsage;
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

	public static <K, T> KeyValue<K, T> of(String event, K key, DataType type, long ttl, T value) {
		KeyValue<K, T> kv = new KeyValue<>();
		kv.setKey(key);
		kv.setEvent(event);
		kv.setTimestamp(System.currentTimeMillis());
		kv.setType(type.getString());
		kv.setTtl(ttl);
		kv.setValue(value);
		return kv;
	}

	public static boolean exists(KeyValue<?, ?> keyValue) {
		return keyValue != null && hasKey(keyValue) && keyValue.getTtl() != TTL_NO_KEY
				&& type(keyValue) != DataType.NONE;
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

}
