package com.redis.spring.batch.writer;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.SimpleBatchWriteOperation;
import com.redis.spring.batch.writer.operation.DumpWriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

public class DumpItemWriter extends KeyValueItemWriter<byte[], byte[]> {

    public DumpItemWriter(AbstractRedisClient client) {
        super(client, ByteArrayCodec.INSTANCE);
    }

    @Override
    protected SimpleBatchWriteOperation<byte[], byte[], KeyValue<byte[]>> batchWriteOperation() {
        return new SimpleBatchWriteOperation<>(new DumpWriteOperation<>());
    }

}
