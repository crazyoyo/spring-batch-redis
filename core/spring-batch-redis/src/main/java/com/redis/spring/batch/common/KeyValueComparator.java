package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.Objects;

import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyValueComparator implements KeyComparator<KeyValue<String>> {

    private final Duration ttlTolerance;

    public KeyValueComparator(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public Status compare(KeyValue<String> source, KeyValue<String> target) {
        if (!target.exists() && source.exists()) {
            return Status.MISSING;
        }
        if (target.getType() != source.getType()) {
            return Status.TYPE;
        }
        if (!Objects.deepEquals(source.getValue(), target.getValue())) {
            return Status.VALUE;
        }
        Duration ttlDiff = Duration.ofMillis(Math.abs(source.getTtl() - target.getTtl()));
        if (ttlDiff.compareTo(ttlTolerance) > 0) {
            return Status.TTL;
        }
        return Status.OK;
    }

}
