package org.springframework.batch.item.redis.support;

import lombok.Getter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public abstract class AbstractKeyspaceNotificationItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<V> {

    private final K[] patterns;
    private final BlockingQueue<V> queue;
    private final long timeout;
    private final BiFunction<K, V, V> function;

    @Getter
    private boolean stopped;

    protected AbstractKeyspaceNotificationItemReader(BlockingQueue<V> queue, long pollingTimeout, K[] patterns, BiFunction<K, V, V> keyExtractor) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.isTrue(pollingTimeout > 0, "Polling timeout must be positive.");
        this.queue = queue;
        this.timeout = pollingTimeout;
        this.patterns = patterns;
        this.function = keyExtractor;
    }

    @Override
    protected void doOpen() {
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
            key = queue.poll(timeout, TimeUnit.MILLISECONDS);
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

}
