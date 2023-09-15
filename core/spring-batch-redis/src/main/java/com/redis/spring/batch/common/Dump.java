package com.redis.spring.batch.common;

public class Dump<K> extends KeyValue<K, byte[]> {

    public Dump(K key, byte[] value) {
        super(key, value);
    }

}
