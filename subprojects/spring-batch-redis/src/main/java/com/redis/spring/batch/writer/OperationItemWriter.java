package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class OperationItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	private final WriteOperation<K, V, T> operation;

	public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, WriteOperation<K, V, T> operation) {
		super(client, codec);
		this.operation = operation;
	}

	@Override
	protected Operation<K, V, T, Object> operation() {
		return operation;
	}

	public static Builder<String, String> builder(AbstractRedisClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static class Builder<K, V> extends RedisItemWriter.BaseBuilder<K, V, Builder<K, V>> {

		protected Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public <T> OperationItemWriter<K, V, T> build(WriteOperation<K, V, T> operation) {
			OperationItemWriter<K, V, T> writer = new OperationItemWriter<>(client, codec, operation);
			configure(writer);
			return writer;
		}

	}
}
