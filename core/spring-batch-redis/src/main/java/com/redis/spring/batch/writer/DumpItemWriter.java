package com.redis.spring.batch.writer;

import com.redis.spring.batch.writer.operation.Restore;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

public class DumpItemWriter extends KeyValueItemWriter<byte[], byte[]> {

	public DumpItemWriter(AbstractRedisClient client) {
		super(client, ByteArrayCodec.INSTANCE);
	}

	@Override
	protected Restore<byte[], byte[]> writeOperation() {
		return new Restore<>();
	}

}
