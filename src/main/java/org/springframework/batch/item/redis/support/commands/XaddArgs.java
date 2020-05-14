package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

import java.util.Map;

public class XaddArgs<K, V> extends KeyArgs<K> {

    @Getter
    private final String id;
    @Getter
    private final Map<K, V> fields;

    public XaddArgs(K key, String id, Map<K, V> fields) {
        super(key);
        this.id = id;
        this.fields = fields;
    }
}