package org.springframework.batch.item.redis.support;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractKeyspaceNotificationItemReader<K, V> extends AbstractPollableItemReader<K> {


    private final K pubSubPattern;
    private final Converter<K, K> keyExtractor;
    private final BlockingQueue<K> queue;
    private boolean stopped;

    protected AbstractKeyspaceNotificationItemReader(K pubSubPattern, Converter<K, K> keyExtractor, int queueCapacity, Duration pollingTimeout) {
        super(pollingTimeout);
        Assert.notNull(pubSubPattern, "A pub/sub subscription pattern is required.");
        Assert.notNull(keyExtractor, "A key extractor is required.");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero.");
        this.pubSubPattern = pubSubPattern;
        this.keyExtractor = keyExtractor;
        this.queue = new ConcurrentSetBlockingQueue<>(queueCapacity);
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    public K poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    protected void doOpen() {
        MetricsUtils.createGaugeCollectionSize("reader.notification.queue.size", queue);
        log.debug("Subscribing to pub/sub pattern {}, queue capacity: {}", pubSubPattern, queue.remainingCapacity());
        subscribe(pubSubPattern);
    }

    @Override
    public boolean isTerminated() {
        return stopped;
    }

    protected abstract void subscribe(K pattern);

    @Override
    protected void doClose() {
        log.debug("Unsubscribing from pub/sub pattern {}", pubSubPattern);
        unsubscribe(pubSubPattern);
        queue.clear();
    }

    protected abstract void unsubscribe(K pubSubPattern);

    protected void notification(K notification, V message) {
        if (notification == null) {
            return;
        }
        K key = keyExtractor.convert(notification);
        if (key == null) {
            return;
        }
        log.debug("Adding key {}", key);
        queue.offer(key);
    }

}
