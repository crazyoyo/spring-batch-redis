package com.redis.spring.batch.common;

public class KeyType<K> {

    private K key;

    private DataStructureType type;

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public DataStructureType getType() {
        return type;
    }

    public void setType(DataStructureType type) {
        this.type = type;
    }

}
