package org.springframework.batch.item.redis.support;

public class KeyValueItemReaderBuilder<B extends KeyValueItemReaderBuilder<B>> extends CommandTimeoutBuilder<B> {

    public static final int DEFAULT_QUEUE_CAPACITY = 1000;
    public static final int DEFAULT_CHUNK_SIZE = 50;
    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final String DEFAULT_KEY_PATTERN = "*";
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final int DEFAULT_SAMPLE_SIZE = 100;
    public static final int DEFAULT_DATABASE = 0;
    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 1000;

    protected int chunkSize = DEFAULT_CHUNK_SIZE;
    protected int threadCount = DEFAULT_THREAD_COUNT;
    protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    protected String keyPattern = DEFAULT_KEY_PATTERN;

    public B keyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
        return (B) this;
    }

    public B chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return (B) this;
    }

    public B threadCount(int threadCount) {
        this.threadCount = threadCount;
        return (B) this;
    }

    public B queueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return (B) this;
    }

}
