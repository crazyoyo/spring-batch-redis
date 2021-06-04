package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue<T> {

	/**
	 * Redis key.
	 *
	 */
	private String key;

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

	public KeyValue(String key) {
		this.key = key;
	}

	public KeyValue(String key, long absoluteTTL) {
		this.key = key;
		this.absoluteTTL = absoluteTTL;
	}

}
