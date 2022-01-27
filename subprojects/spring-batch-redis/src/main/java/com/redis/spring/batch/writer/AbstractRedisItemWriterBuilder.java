package com.redis.spring.batch.writer;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.RedisConnectionBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class AbstractRedisItemWriterBuilder<K, V, T, B extends AbstractRedisItemWriterBuilder<K, V, T, B>>
		extends RedisConnectionBuilder<K, V, B> {

	protected OperationExecutor<K, V, T> executor;

	public AbstractRedisItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
			OperationExecutor<K, V, T> executor) {
		super(client, codec);
		this.executor = executor;
	}

	@SuppressWarnings("unchecked")
	public B multiExec() {
		this.executor = new MultiExecOperationExecutor<>(executor);
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B waitForReplication(int replicas, long timeout) {
		this.executor = new WaitForReplicationOperationExecutor<>(executor, replicas, timeout);
		return (B) this;
	}

	public RedisItemWriter<K, V, T> build() {
		return new RedisItemWriter<>(connectionSupplier(), poolConfig, super.async(), executor);
	}

}