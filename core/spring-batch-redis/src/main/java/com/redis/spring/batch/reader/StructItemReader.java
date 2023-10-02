package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.KeyValueReadOperation.Type;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemReader<K, V> extends KeyValueItemReader<K, V> {

    public StructItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected Operation<K, V, K, KeyValue<K>> operation() {
        KeyValueReadOperation<K, V> operation = new KeyValueReadOperation<>(client, codec, memLimit, memSamples, Type.STRUCT);
        operation.setPostOperator(new StructPostOperator<>(codec));
        return operation;
    }

}
