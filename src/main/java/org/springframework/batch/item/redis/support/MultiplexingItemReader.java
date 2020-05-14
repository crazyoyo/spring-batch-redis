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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MultiplexingItemReader<K> extends AbstractItemStreamItemReader<K> {

    @Getter
    private final List<ItemReader<K>> readers;
    private final BlockingQueue<K> queue;
    private final long timeout;

    private ExecutorService executor;

    @Builder
    public MultiplexingItemReader(List<ItemReader<K>> readers, BlockingQueue<K> queue, long pollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.isTrue(readers != null && !readers.isEmpty(), "No reader specified.");
        Assert.notNull(queue, "A queue instance is required.");
        Assert.isTrue(pollingTimeout > 0, "Polling timeout must be positive.");
        this.readers = readers;
        this.queue = queue;
        this.timeout = pollingTimeout;
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


    @Override
    public K read() throws Exception {
        K key;
        do {
            key = queue.poll(timeout, TimeUnit.MILLISECONDS);
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
