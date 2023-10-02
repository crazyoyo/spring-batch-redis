package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.OperationItemProcessor;

public class KeyComparisonItemProcessor implements ItemProcessor<List<? extends String>, List<KeyComparison>>, ItemStream {

    private final OperationItemProcessor<String, String, String, KeyValue<String>> source;

    private final OperationItemProcessor<String, String, String, KeyValue<String>> target;

    private final long ttlTolerance;

    public KeyComparisonItemProcessor(OperationItemProcessor<String, String, String, KeyValue<String>> source,
            OperationItemProcessor<String, String, String, KeyValue<String>> target, Duration ttlTolerance) {
        this.source = source;
        this.target = target;
        this.ttlTolerance = ttlTolerance.toMillis();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        source.open(executionContext);
        target.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        source.update(executionContext);
        target.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        target.close();
        source.close();
    }

    @Override
    public List<KeyComparison> process(List<? extends String> keys) throws Exception {
        List<KeyValue<String>> sourceItems = source.process(keys);
        List<KeyValue<String>> targetItems = target.process(keys);
        List<KeyComparison> comparisons = new ArrayList<>();
        if (CollectionUtils.isEmpty(sourceItems)) {
            throw new IllegalStateException("No source items found");
        }
        if (CollectionUtils.isEmpty(targetItems)) {
            throw new IllegalStateException("No target items found");
        }
        for (int index = 0; index < sourceItems.size(); index++) {
            KeyComparison comparison = new KeyComparison();
            KeyValue<String> sourceItem = sourceItems.get(index);
            KeyValue<String> targetItem = index < targetItems.size() ? targetItems.get(index) : null;
            comparison.setSource(sourceItem);
            comparison.setTarget(targetItem);
            comparison.setStatus(compare(sourceItem, targetItem));
            comparisons.add(comparison);
        }
        return comparisons;
    }

    private Status compare(KeyValue<String> source, KeyValue<String> target) {
        if (target == null || !target.exists() && source.exists()) {
            return Status.MISSING;
        }
        if (target.getType() != source.getType()) {
            return Status.TYPE;
        }
        if (!Objects.deepEquals(source.getValue(), target.getValue())) {
            return Status.VALUE;
        }
        if (source.getTtl() != target.getTtl()) {
            long delta = Math.abs(source.getTtl() - target.getTtl());
            if (delta > ttlTolerance) {
                return Status.TTL;
            }
        }
        return Status.OK;
    }

}
