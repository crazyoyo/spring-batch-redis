package com.redis.spring.batch.common;

import org.springframework.util.unit.DataSize;

public class KeyValue<K, V> {

    private final K key;

    private final V value;

    /**
     * Expiration POSIX time in milliseconds for this key.
     *
     */
    private long ttl;

    /**
     * Number of bytes that this key and its value require to be stored in Redis RAM. 0 means no memory usage information is
     * available.
     */
    private DataSize memoryUsage;

    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public DataSize getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(DataSize memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

}
