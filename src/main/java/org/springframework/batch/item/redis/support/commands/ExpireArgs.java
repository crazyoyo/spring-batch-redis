package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class ExpireArgs<K> extends KeyArgs<K> {

    @Getter
    private final long timeout;

    public ExpireArgs(K key, long timeout) {
        super(key);
        this.timeout = timeout;
    }
}