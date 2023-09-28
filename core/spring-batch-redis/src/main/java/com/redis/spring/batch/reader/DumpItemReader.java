package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.operation.Evalsha;
import com.redis.spring.batch.reader.operation.KeyValueReadOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class DumpItemReader<K, V> extends AbstractKeyValueItemReader<K, V, KeyValue<K>> {

    private static final String TYPE_NAME = "dump";

    public DumpItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    @Override
    protected Operation<K, V, K, KeyValue<K>> operation(Evalsha<K, V, K> evalsha) {
        return new KeyValueReadOperation<>(codec, evalsha);
    }

    @Override
    protected String typeName() {
        return TYPE_NAME;
    }

}
