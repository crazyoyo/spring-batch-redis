package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue<K, V> {

	/**
	 * Redis key.
	 *
	 */
	private K key;

	/**
	 * Time-to-live in seconds for this key.
	 *
	 */
	private Long ttl;

	/**
	 * Redis value.
	 * 
	 */
	private V value;

	public boolean noKeyTtl() {
		return ttl != null && ttl == -2;
	}

	public boolean hasTtl() {
		return ttl != null && ttl >= 0;
	}

}
