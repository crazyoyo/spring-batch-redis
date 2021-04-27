package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {

    private final ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader;
    /**
     * TTL diff tolerance in milliseseconds
     */
    private final long ttlTolerance;
    private final KeyComparisonResults<K> results;

    @Builder
    public KeyComparisonItemWriter(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader, Duration ttlTolerance) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(valueReader, "A value reader is required");
        Assert.notNull(ttlTolerance, "TTL tolerance is required");
        this.valueReader = valueReader;
        this.ttlTolerance = ttlTolerance.toMillis();
        this.results = new KeyComparisonResults<>();
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        if (valueReader instanceof ItemStream) {
            ((ItemStream) valueReader).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (valueReader instanceof ItemStream) {
            ((ItemStream) valueReader).update(executionContext);
        }
        super.update(executionContext);
    }

    @Override
    public void close() {
        super.close();
        if (valueReader instanceof ItemStream) {
            ((ItemStream) valueReader).close();
        }
    }

    public KeyComparisonResults<K> getResults() {
        return results;
    }

    @Override
    public void write(List<? extends DataStructure<K>> items) throws Exception {
        List<K> keys = items.stream().map(DataStructure::getKey).collect(Collectors.toList());
        List<DataStructure<K>> rightItems = valueReader.process(keys);
        if (rightItems.size() != items.size()) {
            log.warn("Missing values in value reader response");
            return;
        }
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
                        long ttlDiff = Math.abs(left.getAbsoluteTTL() - right.getAbsoluteTTL());
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