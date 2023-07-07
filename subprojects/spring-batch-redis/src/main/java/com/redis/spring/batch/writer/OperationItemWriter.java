package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter.BaseBuilder;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.SimpleBatchOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class OperationItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	private final Operation<K, V, T, Object> operation;

	public OperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec,
			Operation<K, V, T, Object> operation) {
		super(client, codec);
		this.operation = operation;
	}

	@Override
	protected BatchOperation<K, V, T, Object> operation() {
		return new SimpleBatchOperation<>(operation);
	}

	public static Builder<String, String> client(AbstractRedisClient client) {
		return client(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> client(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static class Builder<K, V> extends BaseBuilder<K, V, Builder<K, V>> {

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public <T> OperationItemWriter<K, V, T> operation(Operation<K, V, T, Object> operation) {
			OperationItemWriter<K, V, T> writer = new OperationItemWriter<>(client, codec, operation);
			configure(writer);
			return writer;
		}

	}

}
