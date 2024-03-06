package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class KeyValueItemWriter<K, V> extends RedisItemWriter<K, V, KeyValue<K>> {

    protected KeyValueItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

}
