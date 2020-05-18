package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Builder
public class PoolOptions {

    @Getter
    @Builder.Default
    private final int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
    @Getter
    @Builder.Default
    private final int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
    @Getter
    @Builder.Default
    private final int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;

}
