package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

import java.time.Duration;

public abstract class NotificationKeyValueItemReaderBuilder<B extends NotificationKeyValueItemReaderBuilder<B>> extends KeyValueItemReaderBuilder<B> {

    protected static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
    private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
    public static final Duration DEFAULT_NOTIFICATION_QUEUE_POLLING_TIMEOUT = Duration.ofMillis(100);

    protected int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    protected Duration notificationQueuePollingTimeout = DEFAULT_NOTIFICATION_QUEUE_POLLING_TIMEOUT;
    private int database = DEFAULT_DATABASE;

    public B notificationQueueCapacity(int notificationQueueCapacity) {
        this.notificationQueueCapacity = notificationQueueCapacity;
        return (B) this;
    }

    public B notificationQueuePollingTimeout(Duration notificationQueuePollingTimeout) {
        this.notificationQueuePollingTimeout = notificationQueuePollingTimeout;
        return (B) this;
    }

    public B database(int database) {
        this.database = database;
        return (B) this;
    }

    protected String pubSubPattern() {
        return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
    }

}