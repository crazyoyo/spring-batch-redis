package com.redis.spring.batch.writer;

import java.util.List;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicatedWriteOperation<K, V, T> implements BatchWriteOperation<K, V, T> {

	private final BatchWriteOperation<K, V, T> delegate;
	private final WaitForReplication options;

	public ReplicatedWriteOperation(BatchWriteOperation<K, V, T> delegate, WaitForReplication options) {
		this.delegate = delegate;
		this.options = options;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, List<? extends T> items) {
		delegate.execute(commands, futures, items);
		PipelinedRedisFuture<Void> replicationFuture = new PipelinedRedisFuture<>(
				commands.waitForReplication(options.getReplicas(), options.getTimeout().toMillis()).thenAccept(r -> {
					if (r < options.getReplicas()) {
						throw new RedisCommandExecutionException(String.format(
								"Insufficient replication level - expected: %s, actual: %s", options.getReplicas(), r));
					}
				}));
		futures.add(replicationFuture);
	}

}
