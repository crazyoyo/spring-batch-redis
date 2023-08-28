package com.redis.spring.batch.writer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemWriter<K, V, T> extends AbstractOperationItemWriter<K, V, T> {

    private final Operation<K, V, T> operation;

    public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, T> operation) {
        super(client, codec);
        this.operation = operation;
    }

    @Override
    protected Operation<K, V, T> operation() {
        return operation;
    }

}
