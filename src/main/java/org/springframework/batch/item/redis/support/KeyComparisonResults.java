package org.springframework.batch.item.redis.support;

import lombok.Getter;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class KeyComparisonResults<K> {

    private final AtomicLong ok = new AtomicLong();
    @Getter
    private final List<K> value = new ArrayList<>();
    @Getter
    private final List<K> left = new ArrayList<>();
    @Getter
    private final List<K> right = new ArrayList<>();
    @Getter
    private final List<K> ttl = new ArrayList<>();

    public long getOk() {
        return ok.get();
    }

    public boolean hasDiffs() {
        return !isOk();
    }

    public boolean isOk() {
        return getOk() > 0 && value.isEmpty() && left.isEmpty() && right.isEmpty() && ttl.isEmpty();
    }

    /**
     * Adds the given key to the list of identical keys
     *
     * @param key the key that was compared
     * @return number of keys that are identical on both sides
     */
    public long ok(K key) {
        return ok.incrementAndGet();
    }

    public void left(K key) {
        left.add(key);
    }

    public void right(K key) {
        right.add(key);
    }

    public void ttl(K key) {
        ttl.add(key);
    }

    public void value(K key) {
        value.add(key);
    }

}
