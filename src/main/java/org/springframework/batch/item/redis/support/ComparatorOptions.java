package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;

@Data
@Builder
public class ComparatorOptions {

    public static Duration DEFAULT_TTL_TOLERANCE = Duration.ofSeconds(1);

    @Builder.Default
    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
    @Builder.Default
    private JobOptions jobOptions = JobOptions.builder().build();
}
