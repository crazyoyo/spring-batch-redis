package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.operation.Evalsha;
import com.redis.spring.batch.reader.operation.KeyValueReadOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

public class DumpItemReader extends AbstractKeyValueItemReader<byte[], byte[], KeyValue<byte[]>> {

    private static final String TYPE_NAME = "dump";

    public DumpItemReader(AbstractRedisClient client) {
        super(client, ByteArrayCodec.INSTANCE);
    }

    @Override
    protected KeyValueReadOperation<byte[], byte[]> operation(Evalsha<byte[], byte[], byte[]> evalsha) {
        return new KeyValueReadOperation<>(codec, evalsha);
    }

    @Override
    protected String typeName() {
        return TYPE_NAME;
    }

}
