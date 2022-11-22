package com.redis.spring.batch.writer;

import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.spring.batch.RedisItemWriter;

import io.lettuce.core.api.StatefulConnection;

public class WriterBuilder<K, V, T> {

	private final GenericObjectPool<StatefulConnection<K, V>> connectionPool;
	private PipelinedOperation<K, V, T> operation;
	private WriterOptions options = WriterOptions.builder().build();

	public WriterBuilder(GenericObjectPool<StatefulConnection<K, V>> connectionPool, Operation<K, V, T> operation) {
		this(connectionPool, new SimplePipelinedOperation<>(operation));
	}

	public WriterBuilder(GenericObjectPool<StatefulConnection<K, V>> connectionPool,
			PipelinedOperation<K, V, T> operation) {
		this.connectionPool = connectionPool;
		this.operation = operation;
	}

	public WriterBuilder<K, V, T> options(WriterOptions options) {
		this.options = options;
		return this;
	}

	private PipelinedOperation<K, V, T> operation() {
		Optional<WaitForReplication> waitForReplication = options.getWaitForReplication();
		if (waitForReplication.isPresent()) {
			return new WaitForReplicationOperation<>(operation, waitForReplication.get());
		}
		return operation;
	}

	public RedisItemWriter<K, V, T> build() {
		return new RedisItemWriter<>(connectionPool,
				options.isMultiExec() ? new MultiExecOperation<>(operation()) : operation());
	}

}