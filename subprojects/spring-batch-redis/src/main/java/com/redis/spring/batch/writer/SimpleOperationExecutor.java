package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class SimpleOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private final RedisOperation<K, V, T> operation;

	public SimpleOperationExecutor(RedisOperation<K, V, T> operation) {
		this.operation = operation;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items, List<Future<?>> futures) {
		for (T item : items) {
			RedisFuture<?> future = operation.execute(commands, item);
			if (future == null) {
				continue;
			}
			futures.add(future);
		}
	}

}
