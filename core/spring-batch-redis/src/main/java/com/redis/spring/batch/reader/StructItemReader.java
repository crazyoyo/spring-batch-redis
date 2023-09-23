package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.Struct;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemReader<K, V> extends KeyValueItemReader<K, V, Struct<K>> {

    public StructItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, new StructReadOperation<>(client, codec));
    }

}
