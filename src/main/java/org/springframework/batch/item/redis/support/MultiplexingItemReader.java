package org.springframework.batch.item.redis.support;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class MultiplexingItemReader<K> extends AbstractItemStreamItemReader<K> {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final long DEFAULT_POLLING_TIMEOUT = 100;

    @Getter
    private final List<ItemReader<K>> readers;
    private final int queueCapacity;
    private final Duration pollingTimeout;

    private BlockingQueue<K> queue;
    private long pollingTimeoutInMillis;
    private ExecutorService executor;

    public static class MultiplexingItemReaderBuilder<K> {

        private final List<ItemReader<K>> readers = new ArrayList<>();
        private Integer queueCapacity;
        private Duration pollingTimeout;

        public MultiplexingItemReader<K> build() {
            return new MultiplexingItemReader<>(readers, queueCapacity, pollingTimeout);
        }

        public MultiplexingItemReaderBuilder<K> readers(ItemReader<K>... readers) {
            this.readers.addAll(Arrays.asList(readers));
            return this;
        }

        public MultiplexingItemReaderBuilder<K> queueCapacity(Integer queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public MultiplexingItemReaderBuilder<K> pollingTimeout(Duration pollingTimeout) {
            this.pollingTimeout = pollingTimeout;
            return this;
        }

    }

    public static <K> MultiplexingItemReaderBuilder<K> builder() {
        return new MultiplexingItemReaderBuilder<>();
    }

    public MultiplexingItemReader(List<ItemReader<K>> readers, Integer queueCapacity, Duration pollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        this.readers = readers;
        this.queueCapacity = queueCapacity == null ? DEFAULT_QUEUE_CAPACITY : queueCapacity;
        this.pollingTimeout = pollingTimeout;
    }

    @Override
    public K read() throws Exception {
        K key;
        do {
            key = queue.poll(pollingTimeoutInMillis, TimeUnit.MILLISECONDS);
        } while (key == null && !isDone());
        return key;
    }

    private boolean isDone() {
        return executor.isTerminated();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.pollingTimeoutInMillis = pollingTimeout == null ? DEFAULT_POLLING_TIMEOUT : pollingTimeout.toMillis();
        executor = Executors.newFixedThreadPool(readers.size());
        for (ItemReader<K> reader : readers) {
            if (reader instanceof ItemStream) {
                ((ItemStream) reader).open(executionContext);
            }
            executor.submit(new Enqueuer(reader));
        }
        executor.shutdown();
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

    @Override
    public void close() {
        for (ItemReader<K> reader : readers) {
            if (reader instanceof ItemStream) {
                ((ItemStream) reader).close();
            }
        }
        super.close();
    }
}
