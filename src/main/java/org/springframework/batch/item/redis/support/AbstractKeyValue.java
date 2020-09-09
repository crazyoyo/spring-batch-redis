package org.springframework.batch.item.redis.support;

import lombok.Getter;
import lombok.Setter;

public abstract class AbstractKeyValue<K, V> {

	public final static long TTL_NOT_EXISTS = -2;
	public final static long TTL_NO_EXPIRE = -1;

	/**
	 * Redis key.
	 * 
	 * @param key New key.
	 * @return The current key.
	 */
	@Getter @Setter private K key;
	
	/**
	 * Time-to-live in seconds for this key.
	 * 
	 * @param ttl New TTL in seconds.
	 * @return The current TTL in seconds.
	 */
	@Getter @Setter private long ttl;
	
	/**
	 * Redis value.
	 * 
	 * @param value New value.
	 * @return The current va.
	 */
	@Getter @Setter private V value;

}
