package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.reader.InitializingOperation;
import com.redis.spring.batch.util.BatchUtils;

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
		pool.close();
	}

	public List<O> apply(Iterable<? extends I> items) {
		try (StatefulRedisModulesConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			BaseRedisAsyncCommands<K, V> commands = connection.async();
			List<RedisFuture<O>> futures = new ArrayList<>();
			operation.execute(commands, items, futures);
			connection.flushCommands();
			List<O> out = getAll(connection.getTimeout(), futures);
			connection.setAutoFlushCommands(true);
			return out;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RedisCommandInterruptedException(e);
		} catch (Exception e) {
			throw Exceptions.fromSynchronization(e);
		}
	}

	public static <T> List<T> getAll(Duration timeout, Iterable<RedisFuture<T>> futures)
			throws TimeoutException, InterruptedException, ExecutionException {
		List<T> items = new ArrayList<>();
		long nanos = timeout.toNanos();
		long time = System.nanoTime();
		for (RedisFuture<T> f : futures) {
			if (timeout.isNegative()) {
				items.add(f.get());
			} else {
				if (nanos < 0) {
					throw new TimeoutException(String.format("Timed out after %s", timeout));
				}
				T item = f.get(nanos, TimeUnit.NANOSECONDS);
				items.add(item);
				long now = System.nanoTime();
				nanos -= now - time;
				time = now;
			}
		}
		return items;
	}

}
