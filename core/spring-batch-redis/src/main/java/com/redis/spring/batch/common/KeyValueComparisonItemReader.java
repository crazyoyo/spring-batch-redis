package com.redis.spring.batch.common;

import java.time.Duration;

import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemReader;

public class KeyValueComparisonItemReader extends AbstractKeyComparisonItemReader<KeyValue<String>> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    public KeyValueComparisonItemReader(RedisItemReader<String, String, KeyValue<String>> source,
            RedisItemReader<String, String, KeyValue<String>> target) {
        super(source, target);
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        Assert.notNull(ttlTolerance, "Tolerance must not be null");
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    protected KeyComparator<KeyValue<String>> comparator() {
        return new KeyValueComparator(ttlTolerance);
    }

}
