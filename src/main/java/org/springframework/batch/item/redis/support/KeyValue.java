package org.springframework.batch.item.redis.support;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue<K> {

    private K key;
    private Long ttl;
    private Object value;
    private DataType type;

}
