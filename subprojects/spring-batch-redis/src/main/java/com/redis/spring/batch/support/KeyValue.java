package com.redis.spring.batch.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue<K, T> {

	/**
	 * Redis key.
	 *
	 */
	private K key;

	/**
	 * Expiration POSIX time in milliseconds for this key.
	 *
	 */
	private long absoluteTTL;

	/**
	 * Redis value. Null if key does not exist
	 * 
	 */
	private T value;

	public KeyValue(K key) {
		this.key = key;
	}

	public KeyValue(K key, long absoluteTTL) {
		this.key = key;
		this.absoluteTTL = absoluteTTL;
	}

}
