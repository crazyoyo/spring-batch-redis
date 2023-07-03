package com.redis.spring.batch.common;

import java.util.Objects;

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

	public static boolean isNone(DataStructure<?> item) {
		return NONE.equals(item.getType());
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, memoryUsage, ttl, type);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("rawtypes")
		KeyValue other = (KeyValue) obj;
		return Objects.equals(key, other.key) && memoryUsage == other.memoryUsage && ttl == other.ttl
				&& Objects.equals(type, other.type);
	}

}
