package com.redis.spring.batch.support.operation.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

public class MultiExecOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private OperationExecutor<K, V, T> delegate;

	public MultiExecOperationExecutor(OperationExecutor<K, V, T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public List<Future<?>> execute(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items) {
		List<Future<?>> futures = new ArrayList<>();
		futures.add(commands.multi());
		futures.addAll(delegate.execute(commands, items));
		futures.add(commands.exec());
		return futures;
	}

}
