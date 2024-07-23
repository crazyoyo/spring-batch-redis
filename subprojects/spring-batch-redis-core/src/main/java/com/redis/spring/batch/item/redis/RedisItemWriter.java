package com.redis.spring.batch.item.redis;

import java.time.Duration;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.util.Assert;

import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.common.OperationExecutor;
import com.redis.spring.batch.item.redis.writer.KeyValueRestore;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite.WriteMode;
import com.redis.spring.batch.item.redis.writer.operation.MultiExec;
import com.redis.spring.batch.item.redis.writer.operation.ReplicaWait;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> implements ItemStreamWriter<T> {

	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;
	public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

	private final RedisCodec<K, V> codec;
	private final Operation<K, V, T, Object> operation;

	private AbstractRedisClient client;
	private int waitReplicas;
	private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;
	private boolean multiExec;
	private int poolSize = DEFAULT_POOL_SIZE;

	private OperationExecutor<K, V, T, Object> operationExecutor;

	public RedisItemWriter(RedisCodec<K, V> codec, Operation<K, V, T, Object> operation) {
		this.codec = codec;
		this.operation = operation;
	}

	public Operation<K, V, T, Object> getOperation() {
		return operation;
	}

	public static RedisItemWriter<String, String, KeyValue<String, Object>> struct() {
		return struct(StringCodec.UTF8);
	}

	public static <K, V> RedisItemWriter<K, V, KeyValue<K, Object>> struct(RedisCodec<K, V> codec) {
		return new RedisItemWriter<>(codec, new KeyValueWrite<>());
	}

	public static RedisItemWriter<String, String, KeyValue<String, Object>> struct(WriteMode mode) {
		return struct(StringCodec.UTF8, mode);
	}

	public static <K, V> RedisItemWriter<K, V, KeyValue<K, Object>> struct(RedisCodec<K, V> codec, WriteMode mode) {
		return new RedisItemWriter<>(codec, KeyValueWrite.create(mode));
	}

	public static RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> dump() {
		return new RedisItemWriter<>(ByteArrayCodec.INSTANCE, new KeyValueRestore<>());
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		Assert.notNull(client, "Redis client not set");
		if (operationExecutor == null) {
			operationExecutor = new OperationExecutor<>(codec, operation());
			operationExecutor.setClient(client);
			operationExecutor.setPoolSize(poolSize);
			operationExecutor.open(executionContext);
		}
	}

	@Override
	public synchronized void close() {
		if (operationExecutor != null) {
			operationExecutor.close();
			operationExecutor = null;
		}
	}

	@Override
	public void write(Chunk<? extends T> items) throws Exception {
		operationExecutor.process(items.getItems());
	}

	private Operation<K, V, T, Object> operation() {
		return multiExec(waitReplicas(operation));

	}

	private Operation<K, V, T, Object> waitReplicas(Operation<K, V, T, Object> operation) {
		if (waitReplicas > 0) {
			return new ReplicaWait<>(operation, waitReplicas, waitTimeout);
		}
		return operation;
	}

	private Operation<K, V, T, Object> multiExec(Operation<K, V, T, Object> operation) {
		if (multiExec) {
			return new MultiExec<>(operation);
		}
		return operation;
	}

	public static <T> RedisItemWriter<String, String, T> operation(Operation<String, String, T, Object> operation) {
		return operation(StringCodec.UTF8, operation);
	}

	public static <K, V, T> RedisItemWriter<K, V, T> operation(RedisCodec<K, V> codec,
			Operation<K, V, T, Object> operation) {
		return new RedisItemWriter<>(codec, operation);
	}

	public void setClient(AbstractRedisClient client) {
		this.client = client;
	}

	public int getWaitReplicas() {
		return waitReplicas;
	}

	public void setWaitReplicas(int waitReplicas) {
		this.waitReplicas = waitReplicas;
	}

	public Duration getWaitTimeout() {
		return waitTimeout;
	}

	public void setWaitTimeout(Duration waitTimeout) {
		this.waitTimeout = waitTimeout;
	}

	public boolean isMultiExec() {
		return multiExec;
	}

	public void setMultiExec(boolean multiExec) {
		this.multiExec = multiExec;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

}
