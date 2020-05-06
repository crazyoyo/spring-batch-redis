package org.springframework.batch.item.redis.support;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKeyspaceNotificationItemReader<V> extends AbstractItemCountingItemStreamItemReader<V> {

    public final static int DEFAULT_DATABASE = 0;
    public final static long DEFAULT_POLLING_TIMEOUT = 100;
    public final static int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final String DATABASE_TOKEN_NAME = "database";
    private static final String DATABASE_TOKEN = "${" + DATABASE_TOKEN_NAME + "}";
    private static final String KEYEVENT_CHANNEL_TEMPLATE = "__keyevent@" + DATABASE_TOKEN + "__:*";

    private static String getKeyEventChannel(Integer database) {
        Map<String, String> variables = new HashMap<>();
        variables.put(DATABASE_TOKEN_NAME, String.valueOf(database == null ? DEFAULT_DATABASE : database));
        StringSubstitutor substitutor = new StringSubstitutor(variables);
        return substitutor.replace(KEYEVENT_CHANNEL_TEMPLATE);
    }

    @Getter
    @Setter
    private String channel;
    @Getter
    @Setter
    private BlockingQueue<V> queue;
    @Getter
    @Setter
    private long pollingTimeout;
    @Getter
    private boolean stopped;

    protected AbstractKeyspaceNotificationItemReader(Integer database, BlockingQueue<V> queue, Duration pollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        this.channel = getKeyEventChannel(database);
        this.queue = queue;
        this.pollingTimeout = pollingTimeout == null ? DEFAULT_POLLING_TIMEOUT : pollingTimeout.toMillis();
    }

    protected AbstractKeyspaceNotificationItemReader(Integer database, Integer queueCapacity, Duration pollingTimeout) {
        this(database, createQueue(queueCapacity), pollingTimeout);
    }

    protected static <V> BlockingQueue<V> createQueue(Integer queueCapacity) {
        if (queueCapacity == null) {
            return new LinkedBlockingDeque<>(DEFAULT_QUEUE_CAPACITY);
        }
        return new LinkedBlockingDeque<>(queueCapacity);
    }

    @Override
    protected void doOpen() {
        open(channel);
    }

    protected abstract void open(String channel);

    @Override
    protected void doClose() {
        close(channel);
    }

    protected abstract void close(String channel);

    public void stop() {
        this.stopped = true;
    }

    @Override
    protected V doRead() throws Exception {
        V key;
        do {
            key = queue.poll(pollingTimeout, TimeUnit.MILLISECONDS);
        } while (key == null && !stopped);
        return key;
    }

    protected void message(V message) {
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
