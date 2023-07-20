package com.redis.spring.batch.common;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class ProcessingItemWriter<I, O> extends AbstractItemStreamItemWriter<I> {

    private final ItemProcessor<List<? extends I>, List<O>> processor;

    private final ItemWriter<O> writer;

    public ProcessingItemWriter(ItemProcessor<List<? extends I>, List<O>> processor, ItemWriter<O> writer) {
        this.processor = processor;
        this.writer = writer;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).open(executionContext);
        }
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).update(executionContext);
        }
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).update(executionContext);
        }
    }

    @Override
    public void close() {
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).close();
        }
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).close();
        }
        super.close();
    }

    @Override
    public void write(List<? extends I> items) throws Exception {
        List<O> list = processor.process(items);
        if (list != null) {
            writer.write(list);
        }
    }

}
