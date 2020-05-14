package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class MemberArgs<K, V> extends KeyArgs<K> {

    @Getter
    private final V memberId;

    public MemberArgs(K key, V memberId) {
        super(key);
        this.memberId = memberId;
    }
}
