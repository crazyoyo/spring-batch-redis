package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

import java.time.Duration;

public abstract class NotificationKeyValueItemReaderBuilder<B extends NotificationKeyValueItemReaderBuilder<B>> extends KeyValueItemReaderBuilder<B> {

    protected static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
    private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    private int database = DEFAULT_DATABASE;
    protected int queueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    protected Duration queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

    public B queueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return (B) this;
    }

    public B queuePollingTimeout(Duration queuePollingTimeout) {
        this.queuePollingTimeout = queuePollingTimeout;
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