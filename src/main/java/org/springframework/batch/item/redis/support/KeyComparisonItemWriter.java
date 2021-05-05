package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class KeyComparisonItemWriter<K> extends AbstractItemStreamItemWriter<DataStructure<K>> {

    private final ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader;
    private final List<KeyComparisonListener<K>> listeners = new ArrayList<>();

    @Builder
    public KeyComparisonItemWriter(ItemProcessor<List<? extends K>, List<DataStructure<K>>> valueReader) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(valueReader, "A value reader is required");
        this.valueReader = valueReader;
    }

    public void addListener(KeyComparisonListener<K> listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        Assert.notEmpty(listeners, "No KeyComparisonListener registered");
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

    @Override
    public void write(List<? extends DataStructure<K>> sourceItems) throws Exception {
        List<DataStructure<K>> targetItems = valueReader.process(sourceItems.stream().map(DataStructure::getKey).collect(Collectors.toList()));
        if (targetItems.size() != sourceItems.size()) {
            log.warn("Missing values in value reader response");
            return;
        }
        for (int index = 0; index < sourceItems.size(); index++) {
            DataStructure<K> source = sourceItems.get(index);
            DataStructure<K> target = targetItems.get(index);
            for (KeyComparisonListener<K> listener : listeners) {
                listener.compare(source, target);
            }
        }
    }

}