package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MultiplexingItemReader<K> extends AbstractItemStreamItemReader<K> {

    @Getter
    private final List<ItemReader<K>> readers;
    private final int queueCapacity;
    private final long queuePollingTimeout;

    private ExecutorService executor;
    private LinkedBlockingDeque<K> queue;

    @Builder
    public MultiplexingItemReader(int queueCapacity, long queuePollingTimeout, List<ItemReader<K>> readers) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.isTrue(readers != null && !readers.isEmpty(), "No reader specified.");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be positive.");
        Assert.isTrue(queuePollingTimeout > 0, "Queue polling timeout must be positive.");
        this.readers = readers;
        this.queueCapacity = queueCapacity;
        this.queuePollingTimeout = queuePollingTimeout;
    }


    @Override
    public void open(ExecutionContext executionContext) {
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.executor = Executors.newFixedThreadPool(readers.size());
        for (ItemReader<K> reader : readers) {
            if (reader instanceof ItemStream) {
                ((ItemStream) reader).open(executionContext);
            }
            executor.submit(new Enqueuer(reader));
        }
        executor.shutdown();
        super.open(executionContext);
    }


    @Override
    public void close() {
        super.close();
        for (ItemReader<K> reader : readers) {
            if (reader instanceof ItemStream) {
                ((ItemStream) reader).close();
            }
        }
    }

    @Override
    public K read() throws Exception {
        K key;
        do {
            key = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
        } while (key == null && !isDone());
        return key;
    }

    private boolean isDone() {
        return executor.isTerminated();
    }

    private class Enqueuer implements Runnable {

        private final ItemReader<K> reader;

        public Enqueuer(ItemReader<K> reader) {
            this.reader = reader;
        }

        @Override
        public void run() {
            K key;
            try {
                while ((key = reader.read()) != null) {
                    queue.put(key);
                }
            } catch (Exception e) {
                log.error("Could not read next key", e);
            }
        }
    }

}
