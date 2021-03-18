package org.springframework.batch.item.redis.support;

import lombok.Builder;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class KeyComparisonItemWriter<K, V> extends AbstractItemStreamItemWriter<DataStructure<K>> {

    private final DataStructureItemReader<K, V> valueReader;
    /**
     * TTL diff tolerance in seconds
     */
    private final long ttlTolerance;
    private final KeyComparisonResults<K> results;

    @Builder
    public KeyComparisonItemWriter(DataStructureItemReader<K, V> valueReader, Duration ttlTolerance) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(valueReader, "A value reader is required");
        Assert.notNull(ttlTolerance, "TTL tolerance is required");
        this.valueReader = valueReader;
        this.ttlTolerance = ttlTolerance.getSeconds();
        this.results = new KeyComparisonResults<>();
    }

    public KeyComparisonResults<K> getResults() {
        return results;
    }

    @Override
    public void write(List<? extends DataStructure<K>> items) throws Exception {
        List<K> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
        List<DataStructure<K>> rightItems = valueReader.values(keys);
        for (int index = 0; index < items.size(); index++) {
            DataStructure<K> left = items.get(index);
            K key = left.getKey();
            DataStructure<K> right = rightItems.get(index);
            if (left.getValue() == null) {
                if (right.getValue() == null) {
                    results.ok(key);
                } else {
                    results.right(key);
                }
            } else {
                if (right.getValue() == null) {
                    results.left(key);
                } else {
                    if (Objects.deepEquals(left.getValue(), right.getValue())) {
                        long ttlDiff = Math.abs(left.getTtl() - right.getTtl());
                        if (ttlDiff > ttlTolerance) {
                            results.ttl(key);
                        } else {
                            results.ok(key);
                        }
                    } else {
                        results.value(key);
                    }
                }
            }
        }
    }

}