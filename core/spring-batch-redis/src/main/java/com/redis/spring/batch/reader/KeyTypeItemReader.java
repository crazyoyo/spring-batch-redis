package com.redis.spring.batch.reader;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.operation.KeyTypeReadOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyTypeItemReader<K, V> extends RedisItemReader<K, V, KeyValue<K>> {

    public KeyTypeItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected KeyTypeReadOperation<K, V> operation() {
        return new KeyTypeReadOperation<>();
    }

}
