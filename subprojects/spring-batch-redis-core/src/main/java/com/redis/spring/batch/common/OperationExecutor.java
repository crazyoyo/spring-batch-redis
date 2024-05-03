package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.reader.InitializingOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Exceptions;
import io.lettuce.core.support.ConnectionPoolSupport;

public class OperationExecutor<K, V, I, O> implements InitializingBean, AutoCloseable {

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

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(client, "Redis client not set");
		GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(poolSize);
		Supplier<StatefulRedisModulesConnection<K, V>> supplier = BatchUtils.supplier(client, codec, readFrom);
		pool = ConnectionPoolSupport.createGenericObjectPool(supplier, config);
		if (operation instanceof InitializingOperation) {
			try (StatefulRedisModulesConnection<K, V> connection = pool.borrowObject()) {
				((InitializingOperation<K, V, I, O>) operation).afterPropertiesSet(connection);
			}
		}
	}

	@Override
	public void close() {
		if (pool != null) {
			pool.close();
		}
	}

	public List<O> apply(Iterable<? extends I> items) {
		try (StatefulRedisModulesConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			BaseRedisAsyncCommands<K, V> commands = connection.async();
			List<RedisFuture<O>> futures = new ArrayList<>();
			operation.execute(commands, items, futures);
			connection.flushCommands();
			List<O> out = BatchUtils.getAll(connection.getTimeout(), futures);
			connection.setAutoFlushCommands(true);
			return out;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RedisCommandInterruptedException(e);
		} catch (Exception e) {
			throw Exceptions.fromSynchronization(e);
		}
	}

}
