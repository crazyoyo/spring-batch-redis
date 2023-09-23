package com.redis.spring.batch.reader;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueItemReader<K, V, T extends KeyValue<K, ?>> extends RedisItemReader<K, V, T> {

    public KeyValueItemReader(AbstractRedisClient client, RedisCodec<K, V> codec,
            AbstractKeyValueReadOperation<K, V, T> operation) {
        super(client, codec, operation);
    }

    public void setMemoryUsageLimit(DataSize limit) {
        ((AbstractKeyValueReadOperation<K, V, T>) operation).setMemoryUsageLimit(limit);
    }

    public void setMemoryUsageSamples(int samples) {
        ((AbstractKeyValueReadOperation<K, V, T>) operation).setMemoryUsageSamples(samples);
    }

}
