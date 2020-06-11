package org.springframework.batch.item.redis.support;

public interface KeyListener<K> {

    void key(K key);
}
