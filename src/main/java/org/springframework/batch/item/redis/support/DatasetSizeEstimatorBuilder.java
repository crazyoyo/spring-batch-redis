package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisURI;

import java.time.Duration;

public class DatasetSizeEstimatorBuilder<B extends DatasetSizeEstimatorBuilder<B>> {

    public static final int DEFAULT_SAMPLE_SIZE = 100;

    protected long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
    protected int sampleSize = DEFAULT_SAMPLE_SIZE;
    protected String keyPattern = KeyValueItemReaderBuilder.DEFAULT_KEY_PATTERN;

    public B commandTimeout(Duration commandTimeout) {
        this.commandTimeout = commandTimeout.getSeconds();
        return (B) this;
    }

    public B commandTimeout(long commandTimeout) {
        this.commandTimeout = commandTimeout;
        return (B) this;
    }

    public B sampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
        return (B) this;
    }

    public B keyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
        return (B) this;
    }

}
