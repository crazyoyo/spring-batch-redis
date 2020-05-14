package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class ZaddArgs<K, V> extends MemberArgs<K, V> {

    @Getter
    private final double score;

    public ZaddArgs(K key, V member, double score) {
        super(key, member);
        this.score = score;
    }
}