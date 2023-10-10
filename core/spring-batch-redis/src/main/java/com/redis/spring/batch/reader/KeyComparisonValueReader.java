package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.StreamMessage;

@SuppressWarnings("rawtypes")
public class KeyComparisonValueReader implements ItemProcessor<List<String>, List<KeyComparison>>, ItemStream {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private final ItemProcessor<List<String>, List<KeyValue<String>>> source;

    private final ItemProcessor<List<String>, List<KeyValue<String>>> target;

    private ItemProcessor<KeyValue<String>, KeyValue<String>> processor = new PassThroughItemProcessor<>();

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private boolean compareStreamMessageIds;

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

    public void setCompareStreamMessageIds(boolean enable) {
        this.compareStreamMessageIds = enable;
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
        if (!valueEquals(source, target)) {
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

    @SuppressWarnings("unchecked")
    private boolean valueEquals(KeyValue<String> source, KeyValue<String> target) {
        if (source.getType() == DataType.STREAM) {
            return streamEquals((Collection<StreamMessage>) source.getValue(), (Collection<StreamMessage>) target.getValue());
        }
        return Objects.deepEquals(source.getValue(), target.getValue());
    }

    private boolean streamEquals(Collection<StreamMessage> source, Collection<StreamMessage> target) {
        if (CollectionUtils.isEmpty(source)) {
            return CollectionUtils.isEmpty(target);
        }
        if (source.size() != target.size()) {
            return false;
        }
        Iterator<StreamMessage> sourceIterator = source.iterator();
        Iterator<StreamMessage> targetIterator = target.iterator();
        while (sourceIterator.hasNext()) {
            if (!targetIterator.hasNext()) {
                return false;
            }
            StreamMessage sourceMessage = sourceIterator.next();
            StreamMessage targetMessage = targetIterator.next();
            if (!streamMessageEquals(sourceMessage, targetMessage)) {
                return false;
            }
        }
        return true;
    }

    private boolean streamMessageEquals(StreamMessage sourceMessage, StreamMessage targetMessage) {
        if (!Objects.equals(sourceMessage.getStream(), targetMessage.getStream())) {
            return false;
        }
        if (compareStreamMessageIds && !Objects.equals(sourceMessage.getId(), targetMessage.getId())) {
            return false;
        }
        Map sourceBody = sourceMessage.getBody();
        Map targetBody = targetMessage.getBody();
        if (CollectionUtils.isEmpty(sourceBody)) {
            return CollectionUtils.isEmpty(targetBody);
        }
        return sourceBody.equals(targetBody);
    }

}
