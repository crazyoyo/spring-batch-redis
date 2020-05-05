package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class MultiplexingItemReader<K> extends AbstractItemStreamItemReader<K> {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final long DEFAULT_POLLING_TIMEOUT = 100;
    @Getter
    @Setter
    private List<ItemReader<K>> readers;
    @Getter
    @Setter
    private BlockingQueue<K> queue;
    @Getter
    @Setter
    private long pollingTimeout;

    private ExecutorService executor;

    @Builder
    public MultiplexingItemReader(Integer queueCapacity, Duration pollingTimeout, List<ItemReader<K>> readers) {
        setName(ClassUtils.getShortName(getClass()));
        this.readers = readers;
        this.queue = new LinkedBlockingDeque<>(queueCapacity == null ? DEFAULT_QUEUE_CAPACITY : queueCapacity);
        this.pollingTimeout = pollingTimeout == null ? DEFAULT_POLLING_TIMEOUT : pollingTimeout.toMillis();
    }

    @Override
    public K read() throws Exception {
        K key;
        do {
            key = queue.poll(pollingTimeout, TimeUnit.MILLISECONDS);
        } while (key == null && !isDone());
        return key;
    }

    private boolean isDone() {
        return executor.isTerminated();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);
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
