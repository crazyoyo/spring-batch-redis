package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public class ProcessingItemWriter<K, T> extends AbstractItemStreamItemWriter<K> {

    private final ItemProcessor<List<? extends K>, List<T>> processor;

    private final BlockingQueue<T> queue;

    private boolean open;

    public ProcessingItemWriter(ItemProcessor<List<? extends K>, List<T>> processor, BlockingQueue<T> queue) {
        setName(ClassUtils.getShortName(getClass()));
        this.processor = processor;
        this.queue = queue;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).open(executionContext);
        }
        super.open(executionContext);
        open = true;
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
        if (processor instanceof ItemStream) {
            ((ItemStream) processor).close();
        }
        super.close();
        open = false;
    }

    @Override
    public void write(List<? extends K> items) throws Exception {
        List<T> values = processor.process(items);
        if (values != null) {
            for (T item : values) {
                queue.put(item);
            }
        }
    }

    public boolean isOpen() {
        return open;
    }

    public BlockingQueue<T> getQueue() {
        return queue;
    }

}
