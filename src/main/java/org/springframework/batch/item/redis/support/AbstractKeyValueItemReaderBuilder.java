package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;

import java.time.Duration;

public class AbstractKeyValueItemReaderBuilder<K, V, B extends AbstractKeyValueItemReaderBuilder<K, V, B>> extends CommandTimeoutBuilder<B> {

    public final static Duration DEFAULT_POLLING_TIMEOUT = Duration.ofMillis(100);
    public final static int DEFAULT_QUEUE_CAPACITY = 1000;

    private ItemReader<K> keyReader;
    protected JobOptions jobOptions = JobOptions.builder().build();
    protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    protected Duration pollingTimeout = DEFAULT_POLLING_TIMEOUT;

    protected ItemReader<K> getKeyReader() {
        Assert.notNull(keyReader, "Key reader is required.");
        return keyReader;
    }

    public B keyReader(ItemReader<K> keyReader) {
        this.keyReader = keyReader;
        return (B) this;
    }

    public B jobOptions(JobOptions jobOptions) {
        this.jobOptions = jobOptions;
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
