package com.redis.spring.batch.common;

import java.time.Duration;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyComparisonValueReader;
import com.redis.spring.batch.reader.KeyValueItemReader;

import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private final OperationValueReader<String, String, String, KeyValue<String>> source;

    private final OperationValueReader<String, String, String, KeyValue<String>> target;

    public KeyComparisonItemReader(KeyValueItemReader<String, String> source, KeyValueItemReader<String, String> target) {
        super(source.getClient(), StringCodec.UTF8);
        this.source = source.operationValueReader();
        this.target = target.operationValueReader();
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        if (source != null) {
            source.setName(name + "-source");
        }
        if (target != null) {
            target.setName(name + "-target");
        }
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public KeyComparisonValueReader valueReader() {
        return new KeyComparisonValueReader(source, target, ttlTolerance);
    }

}
