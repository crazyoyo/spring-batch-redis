package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyValue;

public class KeyComparisonValueReader implements ItemProcessor<List<String>, List<KeyComparison>>, ItemStream {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private final ItemProcessor<List<String>, List<KeyValue<String>>> source;

    private final ItemProcessor<List<String>, List<KeyValue<String>>> target;

    private ItemProcessor<KeyValue<String>, KeyValue<String>> processor = new PassThroughItemProcessor<>();

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    public KeyComparisonValueReader(ItemProcessor<List<String>, List<KeyValue<String>>> source,
            ItemProcessor<List<String>, List<KeyValue<String>>> target) {
        this.source = source;
        this.target = target;
    }

    public void setProcessor(ItemProcessor<KeyValue<String>, KeyValue<String>> processor) {
        this.processor = processor;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (source instanceof ItemStream) {
            ((ItemStream) source).open(executionContext);
        }
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).open(executionContext);
        }
        if (target instanceof ItemStream) {
            ((ItemStream) target).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (source instanceof ItemStream) {
            ((ItemStream) source).update(executionContext);
        }
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).update(executionContext);
        }
        if (target instanceof ItemStream) {
            ((ItemStream) target).update(executionContext);
        }

    }

    @Override
    public void close() throws ItemStreamException {
        if (source instanceof ItemStream) {
            ((ItemStream) source).close();
        }
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).close();
        }
        if (target instanceof ItemStream) {
            ((ItemStream) target).close();
        }

    }

    @Override
    public List<KeyComparison> process(List<String> keys) throws Exception {
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
            KeyValue<String> sourceItem = processor.process(sourceItems.get(index));
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
            if (delta > ttlTolerance.toMillis()) {
                return Status.TTL;
            }
        }
        return Status.OK;
    }

}
