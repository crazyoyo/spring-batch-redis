package org.springframework.batch.item.redis.support;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractKeyspaceNotificationItemReader<K, V> extends AbstractPollableItemReader<K> {


    private final K pubSubPattern;
    private final Converter<K, K> notificationToKeyConverter;
    private final BlockingQueue<K> queue;
    private boolean stopped;

    protected AbstractKeyspaceNotificationItemReader(KeyspaceNotificationReaderOptions<K> options) {
        super(options.getQueuePollingTimeout());
        this.pubSubPattern = options.getPubSubPattern();
        this.notificationToKeyConverter = options.getKeyExtractor();
        this.queue = new ConcurrentSetBlockingQueue<>(options.getQueueCapacity());
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    public K poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
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
        K key = notificationToKeyConverter.convert(notification);
        log.debug("Adding key {}", key);
        queue.offer(key);
    }

}
