package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyComparisonItemProcessor;
import com.redis.spring.batch.reader.KeyValueItemReader;

import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private final OperationItemProcessor<String, String, String, KeyValue<String>> source;

    private final OperationItemProcessor<String, String, String, KeyValue<String>> target;

    public KeyComparisonItemReader(KeyValueItemReader<String, String> source, KeyValueItemReader<String, String> target) {
        super(source.getClient(), StringCodec.UTF8);
        this.source = source.operationProcessor();
        this.target = target.operationProcessor();
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
    public ItemProcessor<List<? extends String>, List<KeyComparison>> processor() {
        return new KeyComparisonItemProcessor(source, target, ttlTolerance);
    }

}
