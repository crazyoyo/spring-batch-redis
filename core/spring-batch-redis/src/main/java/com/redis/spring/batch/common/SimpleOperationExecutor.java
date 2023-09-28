package com.redis.spring.batch.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class SimpleOperationExecutor<K, V, I, O> extends AbstractOperationExecutor<K, V, I, O> {

    private final BatchOperation<K, V, I, O> operation;

    public SimpleOperationExecutor(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, I, O> operation) {
        this(client, codec, new SimpleBatchOperation<>(operation));
    }

    public SimpleOperationExecutor(AbstractRedisClient client, RedisCodec<K, V> codec, BatchOperation<K, V, I, O> operation) {
        super(client, codec);
        this.operation = operation;
    }

    @Override
    protected BatchOperation<K, V, I, O> batchOperation() {
        return operation;
    }

}
