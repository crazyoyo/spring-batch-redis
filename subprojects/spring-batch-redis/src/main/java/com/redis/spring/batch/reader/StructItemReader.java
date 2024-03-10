package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.Operation;

import java.io.IOException;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.KeyValueReadOperation.Type;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class StructItemReader<K, V> extends AbstractKeyValueItemReader<K, V> {

	public StructItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}

	@Override
	protected Operation<K, V, K, KeyValue<K>> operation() throws IOException {
		KeyValueReadOperation<K, V> operation = new KeyValueReadOperation<>(getClient(), getCodec(), memLimit,
				memSamples, Type.STRUCT);
		operation.setPostOperator(new StructPostOperator<>(getCodec()));
		return operation;
	}

}
