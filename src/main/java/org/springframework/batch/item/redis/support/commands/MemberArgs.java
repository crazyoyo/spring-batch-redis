package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class MemberArgs<K, V> extends KeyArgs<K> {

    @Getter
    private final V member;

    protected MemberArgs(K key, V member) {
        super(key);
        this.member = member;
    }
}
