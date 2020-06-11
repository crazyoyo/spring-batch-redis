package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReaderOptions {

    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final String DEFAULT_SCAN_MATCH = "*";

    @Builder.Default
    private int threadCount = DEFAULT_THREAD_COUNT;
    @Builder.Default
    private int batchSize = DEFAULT_BATCH_SIZE;
    @Builder.Default
    private QueueOptions valueQueueOptions = QueueOptions.builder().build();
    @Builder.Default
    private QueueOptions keyspaceNotificationQueueOptions = QueueOptions.builder().build();
    @Builder.Default
    private long scanCount = DEFAULT_SCAN_COUNT;
    @Builder.Default
    private String scanMatch = DEFAULT_SCAN_MATCH;
    private boolean live;

}