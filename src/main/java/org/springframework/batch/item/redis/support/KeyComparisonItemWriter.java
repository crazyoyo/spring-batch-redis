package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Getter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KeyComparisonItemWriter<K, V> extends AbstractItemStreamItemWriter<DataStructure<K>> {

    public enum DiffType {
        VALUE, LEFT_ONLY, RIGHT_ONLY, TTL
    }

    private final AtomicLong ok = new AtomicLong();
    @Getter
    private final Map<DiffType, List<K>> diffs = new HashMap<>();
    private final DataStructureItemReader<K, V> valueReader;
    /**
     * TTL diff tolerance in seconds
     */
    private final long ttlTolerance;

    @Builder
    public KeyComparisonItemWriter(DataStructureItemReader<K, V> valueReader, Duration ttlTolerance) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(valueReader, "A value reader is required");
        Assert.notNull(ttlTolerance, "TTL tolerance is required");
        this.valueReader = valueReader;
        for (DiffType type : DiffType.values()) {
            diffs.put(type, new ArrayList<>());
        }
        this.ttlTolerance = ttlTolerance.getSeconds();
    }

    @Override
    public void write(List<? extends DataStructure<K>> items) throws Exception {
        List<K> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
        List<DataStructure<K>> rightItems = valueReader.values(keys);
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

    public boolean hasDiffs() {
        for (List<K> list : diffs.values()) {
            if (!list.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public Map<K, DiffType> allDiffs() {
        Map<K, DiffType> allDiffs = new HashMap<>();
        for (Map.Entry<DiffType, List<K>> list : diffs.entrySet()) {
            list.getValue().forEach(k -> allDiffs.put(k, list.getKey()));
        }
        return allDiffs;
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