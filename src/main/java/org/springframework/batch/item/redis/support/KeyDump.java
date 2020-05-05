package org.springframework.batch.item.redis.support;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyDump<K> {

    private final static Long NON_EXISTENT_KEY_TTL = -2L;
    private final static Long NO_EXPIRE_TTL = -1L;

    private K key;
    /**
     * From Redis documentation: https://redis.io/commands/pttl
     * Returns -2 if the key does not exist.
     * Returns -1 if the key exists but has no associated expire.
     */
    private Long pttl;
    /**
     * DUMP returns null if key does not exist
     */
    private byte[] value;

    public boolean exists() {
        return value != null && !NON_EXISTENT_KEY_TTL.equals(pttl);
    }

    public boolean hasTtl() {
        return pttl != null && !NO_EXPIRE_TTL.equals(pttl);
    }
}