package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.writer.operation.StructWriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemWriter<K, V> extends RedisItemWriter<K, V, KeyValue<K>> {

    private boolean merge;

    public StructItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    @Override
    protected StructWriteOperation<K, V> batchWriteOperation() {
        return new StructWriteOperation<>(!merge);
    }

}
