package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.operation.Evalsha;
import com.redis.spring.batch.reader.operation.StructReadOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemReader<K, V> extends AbstractKeyValueItemReader<K, V, KeyValue<K>> {

    private static final String TYPE_NAME = "struct";

    public StructItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected StructReadOperation<K, V> operation(Evalsha<K, V, K> evalsha) {
        return new StructReadOperation<>(codec, evalsha);
    }

    @Override
    protected String typeName() {
        return TYPE_NAME;
    }

}
