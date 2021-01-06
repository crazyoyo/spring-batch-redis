package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

import java.time.Duration;

public class KeyValueItemReaderBuilder<B extends KeyValueItemReaderBuilder<B>> extends CommandTimeoutBuilder<B> {

    public static final Duration DEFAULT_POLLING_TIMEOUT = Duration.ofMillis(100);
    public static final int DEFAULT_QUEUE_CAPACITY = 1000;
    public static final int DEFAULT_CHUNK_SIZE = 50;
    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final String DEFAULT_KEY_PATTERN = "*";
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final int DEFAULT_DATABASE = 0;
    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 1000;
    public static final Duration DEFAULT_QUEUE_POLLING_TIMEOUT = Duration.ofMillis(100);

    protected int chunkSize = DEFAULT_CHUNK_SIZE;
    protected int threads = DEFAULT_THREAD_COUNT;
    protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    protected Duration pollingTimeout = DEFAULT_POLLING_TIMEOUT;
    protected String keyPattern = DEFAULT_KEY_PATTERN;

    public B keyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
        return (B) this;
    }

    public B chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return (B) this;
    }

    public B threads(int threads) {
        this.threads = threads;
        return (B) this;
    }

    public B queueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return (B) this;
    }

    public B pollingTimeout(Duration pollingTimeout) {
        this.pollingTimeout = pollingTimeout;
        return (B) this;
    }


}
