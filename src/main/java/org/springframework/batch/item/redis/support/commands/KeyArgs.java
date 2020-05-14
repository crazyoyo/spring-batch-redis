package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class KeyArgs<K> {

    @Getter
    private final K key;

    public KeyArgs(K key) {
        this.key = key;
    }

}
