package com.redis.spring.batch.common;

import org.springframework.util.unit.DataSize;

public class KeyValue<K, V> {

    private K key;

    private V value;

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

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
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
