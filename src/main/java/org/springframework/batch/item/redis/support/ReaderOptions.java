package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisURI;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReaderOptions {

    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;
    public static final int DEFAULT_BATCH_SIZE = 50;

    @Builder.Default
    private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
    @Builder.Default
    private int threadCount = DEFAULT_THREAD_COUNT;
    @Builder.Default
    private int batchSize = DEFAULT_BATCH_SIZE;
    @Builder.Default
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    @Builder.Default
    private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;
}