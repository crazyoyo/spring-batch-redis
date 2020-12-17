package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class JobOptions {

    public static final int DEFAULT_THREADS = 1;
    public static final int DEFAULT_CHUNK_SIZE = 50;

    @Builder.Default
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    @Builder.Default
    private int threads = DEFAULT_THREADS;
}
