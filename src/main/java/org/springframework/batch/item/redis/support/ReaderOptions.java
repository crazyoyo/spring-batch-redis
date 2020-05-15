package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

import java.util.List;

@Builder
public class ReaderOptions {

    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final int DEFAULT_THREADS = 1;
    public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 50;

    @Getter
    @Builder.Default
    private final long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;
    @Getter
    @Builder.Default
    private final int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    @Getter
    @Builder.Default
    private final int batchSize = DEFAULT_BATCH_SIZE;
    @Getter
    @Builder.Default
    private final int threads = DEFAULT_THREADS;
    @Getter
    @NonNull
    @Builder.Default
    private final ScanArgs scanArgs = new ScanArgs();
    @Getter
    @Singular
    private final List<String> keyspacePatterns;

}
