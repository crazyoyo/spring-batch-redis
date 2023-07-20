package com.redis.spring.batch.common;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public class QueueItemWriter<T> extends AbstractItemStreamItemWriter<T> {

    private final BlockingQueue<T> queue;

    public QueueItemWriter(BlockingQueue<T> queue) {
        setName(ClassUtils.getShortName(getClass()));
        this.queue = queue;
    }

    @Override
    public void write(List<? extends T> items) throws InterruptedException {
        for (T item : items) {
            queue.put(item);
        }
    }

}
