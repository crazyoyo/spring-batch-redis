package com.redis.spring.batch.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class OperationValueReader<K, V, I, O> extends AbstractOperationExecutor<K, V, I, O> {

    private final BatchOperation<K, V, I, O> operation;

    public OperationValueReader(AbstractRedisClient client, RedisCodec<K, V> codec, BatchOperation<K, V, I, O> operation) {
        super(client, codec);
        this.operation = operation;
    }

    @Override
    protected BatchOperation<K, V, I, O> batchOperation() {
        return operation;
    }

}
