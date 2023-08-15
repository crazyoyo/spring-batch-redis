package com.redis.spring.batch.writer;

import java.util.Collection;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class ProcessingItemWriter<I, O> extends AbstractItemStreamItemWriter<I> {

    private final ItemProcessor<Collection<? extends I>, List<? extends O>> processor;

    private final ItemWriter<O> writer;

    public ProcessingItemWriter(ItemProcessor<Collection<? extends I>, List<? extends O>> reader, ItemWriter<O> writer) {
        this.processor = reader;
        this.writer = writer;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).update(executionContext);
        }
        super.update(executionContext);
    }

    @Override
    public void close() {
        super.close();
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).close();
        }
    }

    @Override
    public void write(List<? extends I> items) throws Exception {
        List<? extends O> targets = processor.process(items);
        if (targets != null) {
            writer.write(targets);
        }
    }

}
