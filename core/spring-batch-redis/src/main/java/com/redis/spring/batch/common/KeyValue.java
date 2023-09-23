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

    public KeyValue() {
    }

    protected KeyValue(Builder<K, V, ?> builder) {
        this.key = builder.key;
        this.value = builder.value;
        this.memoryUsage = builder.memoryUsage;
        this.ttl = builder.ttl;
    }

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

    public static class Builder<K, V, B extends Builder<K, V, B>> {

        private final K key;

        private V value;

        private DataSize memoryUsage;

        private long ttl;

        public Builder(K key) {
            this.key = key;
        }

        @SuppressWarnings("unchecked")
        public B value(V value) {
            this.value = value;
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public B memoryUsage(DataSize size) {
            this.memoryUsage = size;
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public B ttl(long ttl) {
            this.ttl = ttl;
            return (B) this;
        }

    }

}
