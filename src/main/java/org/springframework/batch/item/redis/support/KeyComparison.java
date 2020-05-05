package org.springframework.batch.item.redis.support;

import lombok.*;

import java.util.Arrays;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyComparison<K> {

    public enum Status {
        OK, MISMATCH, MISSING, EXTRA, TTL
    }

    @Getter
    private KeyDump<K> source;
    @Getter
    private KeyDump<K> target;
    @Getter
    private Status status;

    public boolean isOk() {
        return getStatus() == Status.OK;
    }
}
