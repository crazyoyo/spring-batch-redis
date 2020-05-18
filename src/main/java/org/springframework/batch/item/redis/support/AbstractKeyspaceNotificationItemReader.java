package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public abstract class AbstractKeyspaceNotificationItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<V> {

    private final K[] patterns;
    private final int queueCapacity;
    private final long queuePollingTimeout;
    private final BiFunction<K, V, V> function;

    private BlockingQueue<V> queue;
    @Getter
    private boolean stopped;

    protected AbstractKeyspaceNotificationItemReader(K[] patterns, int queueCapacity, long queuePollingTimeout, BiFunction<K, V, V> keyExtractor) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.isTrue(patterns != null && patterns.length > 0, "Patterns are required.");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be positive.");
        Assert.isTrue(queuePollingTimeout > 0, "Queue polling timeout must be positive.");
        Assert.notNull(keyExtractor, "Key extractor is required.");
        this.patterns = patterns;
        this.queueCapacity = queueCapacity;
        this.queuePollingTimeout = queuePollingTimeout;
        this.function = keyExtractor;
    }

    @Override
    protected void doOpen() {
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        open(patterns);
    }

    protected abstract void open(K[] patterns);

    @Override
    protected void doClose() {
        close(patterns);
    }

    protected abstract void close(K[] patterns);

    public void stop() {
        this.stopped = true;
    }

    @Override
    protected V doRead() throws Exception {
        V key;
        do {
            key = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
        } while (key == null && !stopped);
        return key;
    }

    protected void enqueue(K channel, V message) {
        V value = function.apply(channel, message);
        try {
            queue.put(value);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static class KeyspaceNotificationItemReaderBuilder {
        public static final BiFunction<String, String, String> DEFAULT_KEY_EXTRACTOR = (c, m) -> c.substring(c.indexOf(":") + 1);
        public static final int DEFAULT_QUEUE_CAPACITY = 10000;
        public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;
    }

}
