package com.redis.spring.batch.operation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.internal.Exceptions;

public class OperationExecutor<K, V, I, O> implements Function<Iterable<? extends I>, List<O>>, AutoCloseable {

	private final Operation<K, V, I, O> operation;
	private final GenericObjectPool<StatefulRedisModulesConnection<K, V>> pool;

	public OperationExecutor(GenericObjectPool<StatefulRedisModulesConnection<K, V>> pool,
			Operation<K, V, I, O> operation) {
		this.operation = operation;
		this.pool = pool;
	}

	@Override
	public void close() {
		pool.close();
	}

	@Override
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
