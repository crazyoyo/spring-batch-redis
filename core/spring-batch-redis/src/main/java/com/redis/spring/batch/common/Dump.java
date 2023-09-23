package com.redis.spring.batch.common;

public class Dump<K> extends KeyValue<K, byte[]> {

    public Dump() {
    }

    private Dump(Builder<K> builder) {
        super(builder);
    }

    public static <K> Builder<K> key(K key) {
        return new Builder<>(key);
    }

    public static class Builder<K> extends KeyValue.Builder<K, byte[], Builder<K>> {

        public Builder(K key) {
            super(key);
        }

        public Dump<K> build() {
            return new Dump<>(this);
        }

    }

}
