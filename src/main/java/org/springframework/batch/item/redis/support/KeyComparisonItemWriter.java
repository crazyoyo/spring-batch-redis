package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {

    public enum DiffType {
        VALUE, LEFT_ONLY, RIGHT_ONLY, TTL
    }

    private final AtomicLong ok = new AtomicLong();
    @Getter
    private final Map<DiffType, List<K>> diffs = new HashMap<>();
    private final ValueReader<K, DataStructure<K>> right;
    /**
     * TTL diff tolerance in seconds
     */
    private final long ttlTolerance;

    public KeyComparisonItemWriter(ValueReader<K, DataStructure<K>> right, long ttlTolerance) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(right, "A value reader is required.");
        Assert.isTrue(ttlTolerance >= 0, "TTL tolerance must be positive.");
        this.right = right;
        for (DiffType type : DiffType.values()) {
            diffs.put(type, new ArrayList<>());
        }
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public void write(List<? extends DataStructure<K>> items) throws Exception {
        List<K> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
        List<DataStructure<K>> rightItems = right.values(keys);
        for (int index = 0; index < items.size(); index++) {
            DataStructure<K> left = items.get(index);
            DataStructure<K> right = rightItems.get(index);
            DiffType type = compare(left, right);
            if (type == null) {
                ok.incrementAndGet();
            } else {
                diffs.get(type).add(left.getKey());
            }
        }
    }

    public long getOkCount() {
        return ok.get();
    }

    private DiffType compare(DataStructure<K> left, DataStructure<K> right) {
        if (left.getValue() == null) {
            if (right.getValue() == null) {
                return null;
            }
            return DiffType.RIGHT_ONLY;
        }
        if (right.getValue() == null) {
            return DiffType.LEFT_ONLY;
        }
        if (Objects.deepEquals(left.getValue(), right.getValue())) {
            long ttlDiff = Math.abs(left.getTtl() - right.getTtl());
            if (ttlDiff > ttlTolerance) {
                return DiffType.TTL;
            }
            return null;
        }
        return DiffType.VALUE;
    }

}