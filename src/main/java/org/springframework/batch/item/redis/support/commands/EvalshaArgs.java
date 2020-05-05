package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class EvalshaArgs<K, V> {

    @Getter
    private final K[] keys;
    @Getter
    private final V[] args;

    public EvalshaArgs(K[] keys, V[] args) {
        this.keys = keys;
        this.args = args;
    }
}