package com.redis.spring.batch.common;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class ValueReader<K, V, I, O> extends AbstractOperationExecutor<K, V, I, O> {

	private final Operation<K, V, I, O> operation;

	public ValueReader(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, I, O> operation) {
		super(client, codec);
		this.operation = operation;
	}

	@Override
	protected Operation<K, V, I, O> operation() {
		return operation;
	}

}
