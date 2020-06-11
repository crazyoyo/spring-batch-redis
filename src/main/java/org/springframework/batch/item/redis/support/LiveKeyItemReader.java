package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class LiveKeyItemReader<K, V> extends KeyItemReader<K, V> implements KeyListener<K> {

    private final long queuePollingTimeout;
    private final BlockingQueue<K> queue;
    private final KeyspaceNotificationProducer<K> notificationProducer;

    private boolean stopped;
    @Getter
    private boolean running;

    protected LiveKeyItemReader(StatefulConnection<K, V> connection, Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands, ScanArgs scanArgs, int queueCapacity, long queuePollingTimeout, KeyspaceNotificationProducer<K> notificationProducer) {
        super(connection, commands, scanArgs);
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.queuePollingTimeout = queuePollingTimeout;
        this.notificationProducer = notificationProducer;
    }

    @Override
    protected synchronized void doOpen() {
        super.doOpen();
        notificationProducer.addListener(this);
        notificationProducer.open();
        this.running = true;
    }

    @Override
    protected synchronized void doClose() {
        notificationProducer.close();
        notificationProducer.removeListener(this);
        super.doClose();
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    protected synchronized K doRead() throws Exception {
        if (queue.isEmpty()) {
            K key = super.doRead();
            if (key != null) {
                return key;
            }
        }
        K key;
        do {
            key = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
        } while (key == null && !stopped);
        return key;
    }

    @Override
    public void key(K key) {
        if (key == null) {
            return;
        }
        try {
            queue.put(key);
        } catch (InterruptedException e) {
            log.debug("Interrupted while trying to enqueue key", e);
        }
    }

}
