package org.springframework.batch.item.redis;

import lombok.*;
import org.springframework.batch.item.redis.support.AbstractKeyValue;
import org.springframework.batch.item.redis.support.DataType;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue<K> extends AbstractKeyValue<K, Object> {

    private DataType type;

}
