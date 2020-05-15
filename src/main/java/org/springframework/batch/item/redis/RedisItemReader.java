package org.springframework.batch.item.redis;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

    private final ReaderOptions options;
    @Getter
    private final ItemReader<K> keyReader;
    private final ItemProcessor<List<? extends K>, List<? extends T>> valueReader;

    private BlockingQueue<T> valueQueue;
    private ExecutorService executor;
    private List<Batcher<K>> threads;

    public RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<? extends T>> valueReader, ReaderOptions options) {
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(valueReader, "A value reader is required.");
        Assert.notNull(options, "Reader options are required.");
        setName(ClassUtils.getShortName(getClass()));
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.options = options;
    }

    public static RedisItemReaderBuilder builder() {
        return new RedisItemReaderBuilder();
    }

    public static RedisClusterItemReaderBuilder clusterBuilder() {
        return new RedisClusterItemReaderBuilder();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    protected void doOpen() {
        valueQueue = new LinkedBlockingDeque<>(options.getQueueCapacity());
        threads = new ArrayList<>(options.getThreads());
        executor = Executors.newFixedThreadPool(options.getThreads());
        for (int index = 0; index < options.getThreads(); index++) {
            Batcher<K> thread = Batcher.<K>builder().batchSize(options.getBatchSize()).reader(keyReader).writer(ProcessingItemWriter.<K, T>builder().processor(valueReader).writer(QueueItemWriter.<T>builder().queue(valueQueue).build()).build()).build();
            threads.add(thread);
            executor.submit(thread);
        }
        executor.shutdown();
    }

    public void flush() {
        for (Batcher<K> thread : threads) {
            try {
                thread.flush();
            } catch (Exception e) {
                log.error("Could not flush batcher", e);
            }
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).close();
        }
    }

    @Override
    protected void doClose() {
        for (Batcher<K> batcher : threads) {
            batcher.stop();
        }
        try {
            executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interruped while waiting for executor termination");
        }
        threads = null;
        executor = null;
        if (!valueQueue.isEmpty()) {
            log.warn("Closing reader with non-empty value queue");
        }
        valueQueue = null;
    }

    @Override
    protected T doRead() throws Exception {
        T value;
        do {
            value = valueQueue.poll(options.getQueuePollingTimeout(), TimeUnit.MILLISECONDS);
        } while (value == null && !executor.isTerminated());
        return value;
    }

}
