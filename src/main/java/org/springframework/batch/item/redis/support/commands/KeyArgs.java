package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class KeyArgs<K> {

    @Getter
    private final K key;

    protected KeyArgs(K key) {
        this.key = key;
    }

}
