package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue<K, V> {

    public final static long TTL_NOT_EXISTS = -2;
    public final static long TTL_NO_EXPIRE = -1;

    private K key;
    private long ttl;
    private V value;

}
