package org.springframework.batch.item.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.batch.item.redis.support.AbstractKeyValue;

@Data
@EqualsAndHashCode(callSuper = true)
public class KeyDump<K> extends AbstractKeyValue<K, byte[]> {
}
