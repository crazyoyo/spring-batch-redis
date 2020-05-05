package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

import java.util.Map;

public class XaddArgs<K, V> extends KeyArgs<K> {
    @Getter
    private String id;
    @Getter
    private long maxlen;
    @Getter
    private boolean approximateTrimming;
    @Getter
    private Map<K, V> fields;

    protected XaddArgs(K key) {
        super(key);
    }
}