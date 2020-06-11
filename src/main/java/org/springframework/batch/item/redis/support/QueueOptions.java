package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueueOptions {

    public static final int DEFAULT_CAPACITY = 10000;
    public static final long DEFAULT_POLLING_TIMEOUT = 100;

    @Builder.Default
    private int capacity = DEFAULT_CAPACITY;
    @Builder.Default
    private long pollingTimeout = DEFAULT_POLLING_TIMEOUT;
}
