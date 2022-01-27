package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private OperationExecutor<K, V, T> delegate;

	public MultiExecOperationExecutor(OperationExecutor<K, V, T> delegate) {
		this.delegate = delegate;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items, List<Future<?>> futures) {
		futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).multi());
		delegate.execute(commands, items, futures);
		futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).exec());
	}

}
