package com.redis.spring.batch.support.operation.executor;

import java.util.ArrayList;
import java.util.List;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.RedisFuture;

public class MultiExecOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private OperationExecutor<K, V, T> delegate;

	public MultiExecOperationExecutor(OperationExecutor<K, V, T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public List<RedisFuture<?>> execute(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		futures.add(commands.multi());
		futures.addAll(delegate.execute(commands, items));
		futures.add(commands.exec());
		return futures;
	}

}
