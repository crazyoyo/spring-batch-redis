package com.redis.spring.batch.common;

import java.time.Duration;

import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemReader;

public class StructComparisonItemReader extends AbstractKeyComparisonItemReader<Struct<String>> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    public StructComparisonItemReader(RedisItemReader<String, String, Struct<String>> source,
            RedisItemReader<String, String, Struct<String>> target) {
        super(source, target);
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        Assert.notNull(ttlTolerance, "Tolerance must not be null");
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    protected KeyComparator<Struct<String>> comparator() {
        return new StructComparator(ttlTolerance);
    }

}
