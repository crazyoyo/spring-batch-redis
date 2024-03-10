package com.redis.spring.batch.reader;

import java.io.IOException;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.KeyValueReadOperation.Type;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

public class DumpItemReader extends AbstractKeyValueItemReader<byte[], byte[]> {

	public DumpItemReader(AbstractRedisClient client) {
		super(client, ByteArrayCodec.INSTANCE);
	}

	@Override
	protected Operation<byte[], byte[], byte[], KeyValue<byte[]>> operation() throws IOException {
		return new KeyValueReadOperation<>(getClient(), getCodec(), memLimit, memSamples, Type.DUMP);
	}

}
