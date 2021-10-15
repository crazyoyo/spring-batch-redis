package com.redis.spring.batch.support.operation.executor;

import java.util.ArrayList;
import java.util.List;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.RedisOperation;

import io.lettuce.core.RedisFuture;

public class SimpleOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private final RedisOperation<K, V, T> operation;

	public SimpleOperationExecutor(RedisOperation<K, V, T> operation) {
		this.operation = operation;
	}

	@Override
	public List<RedisFuture<?>> execute(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (T item : items) {
			futures.add(operation.execute(commands, item));
		}
		return futures;
	}

}
