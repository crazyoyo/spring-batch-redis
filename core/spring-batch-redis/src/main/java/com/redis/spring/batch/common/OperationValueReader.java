package com.redis.spring.batch.common;

import java.util.List;

import com.redis.spring.batch.reader.ValueReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class OperationValueReader<K, V, I, O> extends AbstractOperationExecutor<K, V, I, O> implements ValueReader<I, O> {

    private final BatchOperation<K, V, I, O> operation;

    public OperationValueReader(AbstractRedisClient client, RedisCodec<K, V> codec, BatchOperation<K, V, I, O> operation) {
        super(client, codec);
        this.operation = operation;
    }

    @Override
    public List<O> process(List<? extends I> keys) throws Exception {
        return execute(keys);
    }

    @Override
    protected BatchOperation<K, V, I, O> batchOperation() {
        return operation;
    }

}
