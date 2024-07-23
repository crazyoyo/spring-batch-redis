package com.redis.spring.batch.item.redis.common;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class OperationExecutor<K, V, I, O> implements ItemStream, ItemProcessor<List<? extends I>, List<O>> {

	public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

	private final Operation<K, V, I, O> operation;
	private final RedisCodec<K, V> codec;

	private AbstractRedisClient client;
	private ReadFrom readFrom;
	private int poolSize = DEFAULT_POOL_SIZE;

	private GenericObjectPool<StatefulRedisModulesConnection<K, V>> pool;

	public OperationExecutor(RedisCodec<K, V> codec, Operation<K, V, I, O> operation) {
		this.codec = codec;
		this.operation = operation;
	}

	public void setClient(AbstractRedisClient client) {
		this.client = client;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		Assert.notNull(client, "Redis client not set");
		initializeOperation();
		GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(poolSize);
		Supplier<StatefulRedisModulesConnection<K, V>> supplier = BatchUtils.supplier(client, codec, readFrom);
		pool = ConnectionPoolSupport.createGenericObjectPool(supplier, config);
	}

	private void initializeOperation() {
		if (operation instanceof InitializingOperation) {
			InitializingOperation<K, V, I, O> initializingOperation = (InitializingOperation<K, V, I, O>) operation;
			initializingOperation.setClient(client);
			try {
				initializingOperation.afterPropertiesSet();
			} catch (Exception e) {
				throw new ItemStreamException(e);
			}
		}
	}

	@Override
	public synchronized void close() {
		if (pool != null) {
			pool.close();
			pool = null;
		}
	}

	@Override
	public List<O> process(List<? extends I> items) throws Exception {
		try (StatefulRedisModulesConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			try {
				return execute(connection, items);
			} catch (RedisNoScriptException e) {
				// Potential fail-over of Redis shard(s). Need to reload the LUA script.
				initializeOperation();
				return execute(connection, items);
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

	private List<O> execute(StatefulRedisModulesConnection<K, V> connection, List<? extends I> items)
			throws TimeoutException, InterruptedException, ExecutionException {
		List<RedisFuture<O>> futures = operation.execute(connection.async(), items);
		connection.flushCommands();
		return BatchUtils.getAll(connection.getTimeout(), futures);
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public int getPoolSize() {
		return poolSize;
	}

}
