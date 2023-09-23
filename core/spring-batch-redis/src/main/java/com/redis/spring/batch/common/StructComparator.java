package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.Objects;

import com.redis.spring.batch.common.KeyComparison.Status;

public class StructComparator implements KeyComparator<Struct<String>> {

    private final Duration ttlTolerance;

    public StructComparator(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public Status compare(Struct<String> source, Struct<String> target) {
        if (target == null) {
            return Status.MISSING;
        }
        if (!Objects.equals(source.getType(), target.getType())) {
            if (target.getType() == DataStructureType.NONE) {
                return Status.MISSING;
            }
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
