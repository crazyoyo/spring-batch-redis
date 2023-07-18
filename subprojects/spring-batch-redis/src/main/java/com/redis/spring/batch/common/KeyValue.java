package com.redis.spring.batch.common;

public class KeyValue<K> {

	public static final String NONE = "none";
	public static final String HASH = "hash";
	public static final String JSON = "ReJSON-RL";
	public static final String LIST = "list";
	public static final String SET = "set";
	public static final String STREAM = "stream";
	public static final String STRING = "string";
	public static final String TIMESERIES = "TSDB-TYPE";
	public static final String ZSET = "zset";

	/**
	 * Redis key.
	 *
	 */
	private K key;

	/**
	 * Value stored at key. Null if key does not exist.
	 */
	protected Object value;

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private long ttl;

	private String type = NONE;

	/**
	 * Number of bytes that this key and its value require to be stored in Redis
	 * RAM. Null or -1 means no memory usage information is available.
	 */
	private long memoryUsage;

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

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	@SuppressWarnings("unchecked")
	public <T> T getValue() {
		return (T) value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public static boolean isNone(KeyValue<?> item) {
		return isType(item, NONE);
	}

	public static boolean isType(KeyValue<?> item, String type) {
		return type.equals(item.getType());
	}

	public static boolean isString(KeyValue<?> item) {
		return isType(item, STRING);
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", ttl=" + ttl + ", type=" + type + ", memoryUsage=" + memoryUsage + "]";
	}

}
