package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public abstract class AbstractLiveKeyItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyItemReader<K, V, C> {

    private final LiveKeyReaderOptions<K> options;
    private BlockingQueue<K> queue;
    @Getter
    private boolean stopped;

    protected AbstractLiveKeyItemReader(C connection, Function<C, BaseRedisCommands<K, V>> commands, LiveKeyReaderOptions<K> options) {
        super(connection, commands, options.getScanArgs());
        this.options = options;
    }

    @Override
    protected synchronized void doOpen() {
        this.queue = new LinkedBlockingDeque<>(options.getQueueCapacity());
        open(options.getPubsubPattern());
        super.doOpen();
    }

    protected abstract void open(K channelPattern);

    public void stop() {
        this.stopped = true;
    }

    @Override
    protected synchronized void doClose() {
        super.doClose();
        close(options.getPubsubPattern());
    }

    protected abstract void close(K channelPattern);

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
            key = queue.poll(options.getQueuePollingTimeout(), TimeUnit.MILLISECONDS);
        } while (key == null && !stopped);
        return key;
    }

    protected void enqueue(K channel) {
        K key = options.getChannelConverter().convert(channel);
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
