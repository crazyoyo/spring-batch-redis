package org.springframework.batch.item.redis.support;

import lombok.Data;

@Data
public class KeyValue<T> {

	/**
	 * Redis key.
	 * 
	 * @param key New key.
	 * @return The current key.
	 */
	private String key;

	/**
	 * Time-to-live in seconds for this key.
	 * 
	 * @param ttl New TTL in seconds.
	 * @return The current TTL in seconds.
	 */
	private Long ttl;

	/**
	 * Redis value.
	 * 
	 * @param value New value.
	 * @return The current va.
	 */
	private T value;

	public KeyValue() {
	}

	public KeyValue(String key) {
		this.key = key;
	}

	public KeyValue(String key, long ttl) {
		this.key = key;
		this.ttl = ttl;
	}

	public KeyValue(String key, long ttl, T value) {
		this.key = key;
		this.ttl = ttl;
		this.value = value;
	}

	public boolean noKeyTtl() {
		return ttl != null && ttl == -2;
	}

	public boolean hasTtl() {
		return ttl != null && ttl >= 0;
	}

}
