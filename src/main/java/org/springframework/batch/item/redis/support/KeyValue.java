package org.springframework.batch.item.redis.support;

import lombok.Data;

@Data
public class KeyValue<K, V> {

    /**
     * Redis key.
     * 
     * @param key New key.
     * @return The current key.
     */
    private K key;

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
    private V value;

    public boolean noKeyTtl() {
	return ttl != null && ttl == -2;
    }

    public boolean hasTtl() {
	return ttl != null && ttl >= 0;
    }

}
