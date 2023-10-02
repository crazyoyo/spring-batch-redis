package com.redis.spring.batch.common;

import java.time.Duration;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.AbstractKeyValueItemReader;
import com.redis.spring.batch.reader.KeyComparisonItemProcessor;

import io.lettuce.core.codec.StringCodec;

public class KeyComparisonItemReader extends RedisItemReader<String, String, KeyComparison> {

    public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

    private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;

    private final AbstractKeyValueItemReader<String, String> source;

    private final AbstractKeyValueItemReader<String, String> target;

    public KeyComparisonItemReader(AbstractKeyValueItemReader<String, String> source,
            AbstractKeyValueItemReader<String, String> target) {
        super(source.getClient(), StringCodec.UTF8);
        this.source = source;
        this.target = target;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

    @Override
    public ItemProcessor<List<? extends String>, List<KeyComparison>> processor() {
        return new KeyComparisonItemProcessor(source.operationProcessor(), target.operationProcessor(), ttlTolerance);
    }

}
