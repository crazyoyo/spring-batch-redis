package org.springframework.batch.item.redis.support;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class KeyDump<K> extends AbstractKeyValue<K, byte[]> {
}
