package com.redis.spring.batch.reader;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class ProcessingItemWriter<K, T> extends AbstractItemStreamItemWriter<K> {

    private final ItemProcessor<List<? extends K>, List<T>> processor;

    private final ItemWriter<T> writer;

    public ProcessingItemWriter(ItemProcessor<List<? extends K>, List<T>> processor, ItemWriter<T> writer) {
        this.processor = processor;
        this.writer = writer;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).open(executionContext);
        }
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).update(executionContext);
        }
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).update(executionContext);
        }
        super.update(executionContext);
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
    public void write(List<? extends K> items) throws Exception {
        List<T> values = processor.process(items);
        if (values != null) {
            writer.write(values);
        }
    }

}
