package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.Future;

import com.redis.spring.batch.RedisItemWriter.WaitForReplication;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class WaitForReplicationOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private final OperationExecutor<K, V, T> delegate;
	private final WaitForReplication options;

	public WaitForReplicationOperationExecutor(OperationExecutor<K, V, T> delegate, WaitForReplication options) {
		this.delegate = delegate;
		this.options = options;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items, List<Future<?>> futures) {
		delegate.execute(commands, items, futures);
		futures.add(commands.waitForReplication(options.getReplicas(), options.getTimeout().toMillis())
				.toCompletableFuture().thenAccept(r -> {
					if (r < options.getReplicas()) {
						throw new RedisCommandExecutionException(String.format(
								"Insufficient replication level - expected: %s, actual: %s", options.getReplicas(), r));
					}
				}));
	}

}
