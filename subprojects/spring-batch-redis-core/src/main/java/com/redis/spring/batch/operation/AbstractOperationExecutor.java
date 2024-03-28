package com.redis.spring.batch.operation;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.Chunk;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;

import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Exceptions;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractOperationExecutor<K, V, I, O> implements AutoCloseable {

	public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	public static final int DEFAULT_MAX_ATTEMPTS = 3;
	public static final RetryPolicy DEFAULT_RETRY_POLICY = new MaxAttemptsRetryPolicy();

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private ReadFrom readFrom;
	private int poolSize = DEFAULT_POOL_SIZE;
	private GenericObjectPool<StatefulConnection<K, V>> pool;
	private Operation<K, V, I, O> operation;

	protected AbstractOperationExecutor(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public synchronized void open() {
		if (pool == null) {
			Supplier<StatefulConnection<K, V>> connectionSupplier = ConnectionUtils.supplier(client, codec, readFrom);
			GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
			config.setMaxTotal(poolSize);
			operation = operation();
			pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, config);
		}
	}

	protected abstract Operation<K, V, I, O> operation();

	@Override
	public synchronized void close() {
		if (pool != null) {
			pool.close();
			pool = null;
		}
	}

	public O execute(I item) {
		return execute(new Chunk<>(item)).getItems().get(0);
	}

	public Chunk<O> execute(Chunk<? extends I> items) {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			BaseRedisAsyncCommands<K, V> commands = ConnectionUtils.async(connection);
			Chunk<RedisFuture<O>> futures = new Chunk<>();
			operation.execute(commands, items, futures);
			connection.flushCommands();
			Chunk<O> out = getAll(connection.getTimeout(), futures);
			connection.setAutoFlushCommands(true);
			return out;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RedisCommandInterruptedException(e);
		} catch (Exception e) {
			throw Exceptions.fromSynchronization(e);
		}
	}

	public static <T> Chunk<T> getAll(Duration timeout, Chunk<RedisFuture<T>> futures)
			throws TimeoutException, InterruptedException, ExecutionException {
		Chunk<T> items = new Chunk<>();
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
				if (item != null) {
					items.add(item);
				}
				long now = System.nanoTime();
				nanos -= now - time;
				time = now;
			}
		}
		return items;
	}

}
