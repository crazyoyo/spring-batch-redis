package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.Dump;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class DumpItemWriter<K, V> extends RedisItemWriter<K, V, Dump<K>> {

    public DumpItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, new DumpWriteOperation<>());
    }

}
