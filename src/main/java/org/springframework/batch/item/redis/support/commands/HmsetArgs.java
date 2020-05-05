package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

import java.util.Map;

public class HmsetArgs<K, V> extends KeyArgs<K> {

    @Getter
    private final Map<K, V> map;

    public HmsetArgs(K key, Map<K, V> map) {
        super(key);
        this.map = map;
    }

}