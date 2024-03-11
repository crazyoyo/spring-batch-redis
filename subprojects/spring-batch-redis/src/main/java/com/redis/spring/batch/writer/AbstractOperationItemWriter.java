package com.redis.spring.batch.writer;

import java.time.Duration;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamWriter;

import com.redis.spring.batch.common.AbstractOperationExecutor;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.writer.operation.MultiExec;
import com.redis.spring.batch.writer.operation.ReplicaWait;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractOperationItemWriter<K, V, T> extends AbstractOperationExecutor<K, V, T, Object>
		implements ItemStreamWriter<T> {

	public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

	private int waitReplicas;
	private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;
	private boolean multiExec;

	protected AbstractOperationItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}

	public void setWaitReplicas(int replicas) {
		this.waitReplicas = replicas;
	}

	public void setWaitTimeout(Duration timeout) {
		this.waitTimeout = timeout;
	}

	public void setMultiExec(boolean enabled) {
		this.multiExec = enabled;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		open();
	}

	@Override
	public void write(Chunk<? extends T> items) throws RedisException {
		execute(items);
	}

	@Override
	protected Operation<K, V, T, Object> operation() {
		Operation<K, V, T, Object> operation = writeOperation();
		operation = replicaWaitOperation(operation);
		return multiExec(operation);
	}

	protected abstract Operation<K, V, T, Object> writeOperation();

	private Operation<K, V, T, Object> multiExec(Operation<K, V, T, Object> operation) {
		if (multiExec) {
			return new MultiExec<>(operation);
		}
		return operation;
	}

	private Operation<K, V, T, Object> replicaWaitOperation(Operation<K, V, T, Object> operation) {
		if (waitReplicas > 0) {
			return new ReplicaWait<>(operation, waitReplicas, waitTimeout);
		}
		return operation;
	}

}
