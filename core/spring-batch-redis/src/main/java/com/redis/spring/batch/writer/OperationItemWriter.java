package com.redis.spring.batch.writer;

import com.redis.spring.batch.common.SimpleBatchWriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemWriter<K, V, T> extends AbstractOperationItemWriter<K, V, T> {

    private final BatchWriteOperation<K, V, T> operation;

    public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, WriteOperation<K, V, T> operation) {
        this(client, codec, new SimpleBatchWriteOperation<>(operation));
    }

    public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, BatchWriteOperation<K, V, T> operation) {
        super(client, codec);
        this.operation = operation;
    }

    @Override
    protected BatchWriteOperation<K, V, T> batchWriteOperation() {
        return operation;
    }

}
