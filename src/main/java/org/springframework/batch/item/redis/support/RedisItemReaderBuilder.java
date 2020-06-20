package org.springframework.batch.item.redis.support;

@SuppressWarnings("unchecked")
public class RedisItemReaderBuilder<B extends RedisItemReaderBuilder<B, T>, T> extends RedisConnectionBuilder<B> {

    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final String DEFAULT_SCAN_MATCH = "*";
    public static final int DEFAULT_CAPACITY = 10000;
    public static final long DEFAULT_POLLING_TIMEOUT = 100;

    protected int threadCount = DEFAULT_THREAD_COUNT;
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected int queueCapacity = DEFAULT_CAPACITY;
    protected long queuePollingTimeout = DEFAULT_POLLING_TIMEOUT;
    protected long scanCount = DEFAULT_SCAN_COUNT;
    protected String scanMatch = DEFAULT_SCAN_MATCH;
    private boolean live;

    public B threads(int threads) {
        this.threadCount = threads;
        return (B) this;
    }

    public B batch(int batch) {
        this.batchSize = batch;
        return (B) this;
    }

    public B queueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return (B) this;
    }

    public B queuePollingTimeout(long queuePollingTimeout) {
        this.queuePollingTimeout = queuePollingTimeout;
        return (B) this;
    }

    public B scanCount(long scanCount) {
        this.scanCount = scanCount;
        return (B) this;
    }

    public B scanMatch(String scanMatch) {
        this.scanMatch = scanMatch;
        return (B) this;
    }

    public B live() {
        this.live = true;
        return (B) this;
    }

    protected KeyItemReader<String, String> keyReader() {
        if (live) {
            String pubSubPattern = "__keyspace@" + getRedisURI().getDatabase() + "__:" + scanMatch;
            return new LiveKeyItemReader<>(connection(), sync(), scanCount, scanMatch, pubSubConnection(), queueCapacity, queuePollingTimeout, pubSubPattern, new StringChannelConverter());
        }
        return new KeyItemReader<>(connection(), sync(), scanCount, scanMatch);
    }

}
