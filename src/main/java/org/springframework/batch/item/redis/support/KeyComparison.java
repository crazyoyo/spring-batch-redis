package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KeyComparison<K> {

    public enum Result {
        OK, VALUE, LEFT_ONLY, RIGHT_ONLY, TTL
    }

    private K key;
    private Result result;


}
