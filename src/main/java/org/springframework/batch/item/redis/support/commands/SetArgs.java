package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class SetArgs<K, V> extends KeyArgs<K> {

    @Getter
    private final V value;

    public SetArgs(K key, V value) {
        super(key);
        this.value = value;
    }
}