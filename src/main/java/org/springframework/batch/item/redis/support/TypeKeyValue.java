package org.springframework.batch.item.redis.support;

import lombok.*;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class TypeKeyValue<K> extends KeyValue<K, Object> {

    private DataType type;

}
