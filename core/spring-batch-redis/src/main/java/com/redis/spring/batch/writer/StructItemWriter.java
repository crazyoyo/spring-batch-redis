package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.Struct;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemWriter<K, V> extends RedisItemWriter<K, V, Struct<K>> {

    public StructItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, new StructWriteOperation<>());
    }

    public void setMerge(boolean merge) {
        ((StructWriteOperation<K, V>) operation).setMerge(merge);
    }

}
