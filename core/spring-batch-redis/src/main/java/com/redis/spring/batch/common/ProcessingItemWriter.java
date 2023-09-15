package com.redis.spring.batch.common;

import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class ProcessingItemWriter<I, O> extends AbstractItemStreamItemWriter<I> {

    private final Function<List<? extends I>, List<O>> function;

    private final ItemWriter<O> writer;

    public ProcessingItemWriter(Function<List<? extends I>, List<O>> function, ItemWriter<O> writer) {
        this.function = function;
        this.writer = writer;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).update(executionContext);
        }
        super.update(executionContext);
    }

    @Override
    public void close() {
        super.close();
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).close();
        }
    }

    @Override
    public void write(List<? extends I> items) throws Exception {
        List<? extends O> targets = function.apply(items);
        if (targets != null) {
            writer.write(targets);
        }
    }

}
