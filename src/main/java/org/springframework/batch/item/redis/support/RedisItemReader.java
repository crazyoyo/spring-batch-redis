package org.springframework.batch.item.redis.support;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;
    @Getter
    private final ItemProcessor<List<? extends K>, List<T>> keyValueProcessor;
    private final BlockingQueue<T> itemQueue;
    private final ExecutorService executor;
    private final List<BatchRunnable<K>> enqueuers;
    private final long queuePollingTimeout;

    public RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueProcessor, int threadCount, int batchSize, int queueCapacity, long queuePollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(valueProcessor, "A key/value processor is required.");
        this.keyReader = keyReader;
        this.keyValueProcessor = valueProcessor;
        this.itemQueue = new LinkedBlockingDeque<>(queueCapacity);
        this.queuePollingTimeout = queuePollingTimeout;
        this.executor = Executors.newFixedThreadPool(threadCount);
        this.enqueuers = new ArrayList<>(threadCount);
        for (int index = 0; index < threadCount; index++) {
            enqueuers.add(new BatchRunnable<K>(keyReader, this::write, batchSize));
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).close();
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).update(executionContext);
        }
    }

    @Override
    protected void doOpen() {
        enqueuers.forEach(executor::submit);
        executor.shutdown();
    }

    private void write(List<? extends K> keys) throws Exception {
        List<T> values = keyValueProcessor.process(keys);
        if (values == null) {
            return;
        }
        itemQueue.addAll(values);
    }

    @Override
    protected void doClose() throws ItemStreamException {
        if (executor.isTerminated()) {
            return;
        }
        executor.shutdownNow();
    }

    public void flush() {
        for (BatchRunnable<K> enqueuer : enqueuers) {
            try {
                enqueuer.flush();
            } catch (Exception e) {
                log.error("Could not flush", e);
            }
        }
    }

    @Override
    protected T doRead() throws Exception {
        T item;
        do {
            item = itemQueue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
        } while (item == null && !executor.isTerminated());
        return item;
    }

}
