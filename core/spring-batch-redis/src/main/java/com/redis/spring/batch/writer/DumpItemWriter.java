package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.SimpleBatchWriteOperation;
import com.redis.spring.batch.writer.operation.DumpWriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class DumpItemWriter<K, V> extends RedisItemWriter<K, V, KeyValue<K>> {

    public DumpItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected BatchWriteOperation<K, V, KeyValue<K>> batchWriteOperation() {
        return new SimpleBatchWriteOperation<>(new DumpWriteOperation<>());
    }

}
