package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class BlockingQueueItemWriter<T> extends AbstractItemStreamItemWriter<T> {

    private final BlockingQueue<T> queue;

    public BlockingQueueItemWriter(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        for (T item : items) {
            queue.put(item);
        }
    }

}
