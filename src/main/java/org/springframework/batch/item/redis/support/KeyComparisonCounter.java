package org.springframework.batch.item.redis.support;

import lombok.Builder;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Builder
public class KeyComparisonCounter<K> implements KeyComparisonListener<K> {

    private static final long DEFAULT_TTL_TOLERANCE = 100;

    @Builder.Default
    private long ttlTolerance = DEFAULT_TTL_TOLERANCE;
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong sourceOnly = new AtomicLong();
    private final AtomicLong targetOnly = new AtomicLong();
    private final AtomicLong ttl = new AtomicLong();
    private final AtomicLong value = new AtomicLong();

    @Override
    public void compare(DataStructure<K> source, DataStructure<K> target) {
        getBucket(source, target).incrementAndGet();
    }

    private AtomicLong getBucket(DataStructure<K> source, DataStructure<K> target) {
        if (source.getValue() == null) {
            if (target.getValue() == null) {
                return ok;
            }
            return targetOnly;
        }
        if (target.getValue() == null) {
            return sourceOnly;
        }
        if (Objects.deepEquals(source.getValue(), target.getValue())) {
            if (Math.abs(source.getAbsoluteTTL() - target.getAbsoluteTTL()) > ttlTolerance) {
                return ttl;
            }
            return ok;
        }
        return value;
    }

    public boolean isOK() {
        return ok.get() > 0 && value.get() == 0 && sourceOnly.get() == 0 && targetOnly.get() == 0 && ttl.get() == 0;
    }

    public long getOK() {
        return ok.get();
    }

    public long getValue() {
        return value.get();
    }

    public long getSourceOnly() {
        return sourceOnly.get();
    }

    public long getTargetOnly() {
        return targetOnly.get();
    }

    public long getTTL() {
        return ttl.get();
    }
}
