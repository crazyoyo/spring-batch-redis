package com.redis.spring.batch.writer;

import com.redis.spring.batch.writer.operation.StructBatchWriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemWriter<K, V> extends KeyValueItemWriter<K, V> {

    private boolean merge;

    public StructItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    @Override
    protected StructBatchWriteOperation<K, V> batchWriteOperation() {
        return new StructBatchWriteOperation<>(!merge);
    }

}
