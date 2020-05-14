package org.springframework.batch.item.redis.support.commands;

import lombok.Getter;

public class GeoaddArgs<K, V> extends MemberArgs<K, V> {

    @Getter
    private final double longitude;
    @Getter
    private final double latitude;

    public GeoaddArgs(K key, V member, double longitude, double latitude) {
        super(key, member);
        this.longitude = longitude;
        this.latitude = latitude;
    }
}