package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.ItemWriter;

public class QueueItemWriter<T> implements ItemWriter<T> {

    private final BlockingQueue<T> queue;

    public QueueItemWriter(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        for (T item : items) {
            queue.put(item);
        }

    }

}
