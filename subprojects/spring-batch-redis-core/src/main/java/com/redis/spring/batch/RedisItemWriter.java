package com.redis.spring.batch;

import java.time.Duration;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.writer.KeyValueRestore;
import com.redis.spring.batch.writer.KeyValueWrite;
import com.redis.spring.batch.writer.MultiExec;
import com.redis.spring.batch.writer.ReplicaWait;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;
	public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(1);

	private final RedisCodec<K, V> codec;
	private final WriteOperation<K, V, T> operation;

	private AbstractRedisClient client;
	private int waitReplicas;
	private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;
	private boolean multiExec;
	private int poolSize = DEFAULT_POOL_SIZE;

	private OperationExecutor<K, V, T, Object> executor;

	public RedisItemWriter(RedisCodec<K, V> codec, WriteOperation<K, V, T> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.codec = codec;
		this.operation = operation;
	}

	public Operation<K, V, T, Object> getOperation() {
		return operation;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		Assert.notNull(client, "Redis client not set");
		if (executor == null) {
			executor = new OperationExecutor<>(codec, operation());
			executor.setClient(client);
			executor.setPoolSize(poolSize);
			try {
				executor.afterPropertiesSet();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize operation executor", e);
			}
		}
	}

	@Override
	public synchronized void close() {
		if (executor != null) {
			executor.close();
			executor = null;
		}
	}

	@Override
	public void write(Chunk<? extends T> items) {
		executor.apply(items);
	}

	private WriteOperation<K, V, T> operation() {
		WriteOperation<K, V, T> actualOperation = operation;
		if (waitReplicas > 0) {
			actualOperation = new ReplicaWait<>(actualOperation, waitReplicas, waitTimeout);
		}
		if (multiExec) {
			actualOperation = new MultiExec<>(actualOperation);
		}
		return actualOperation;
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

	public static RedisItemWriter<String, String, KeyValue<String, Object>> struct() {
		return struct(StringCodec.UTF8);
	}

	public static <K, V> RedisItemWriter<K, V, KeyValue<K, Object>> struct(RedisCodec<K, V> codec) {
		return new RedisItemWriter<>(codec, new KeyValueWrite<>());
	}

	public static RedisItemWriter<byte[], byte[], KeyValue<byte[], byte[]>> dump() {
		return new RedisItemWriter<>(ByteArrayCodec.INSTANCE, new KeyValueRestore<>());
	}

	public static <T> RedisItemWriter<String, String, T> operation(WriteOperation<String, String, T> operation) {
		return operation(StringCodec.UTF8, operation);
	}

	public static <K, V, T> RedisItemWriter<K, V, T> operation(RedisCodec<K, V> codec,
			WriteOperation<K, V, T> operation) {
		return new RedisItemWriter<>(codec, operation);
	}

}
