package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class WaitForReplicationOperation<K, V, T> implements PipelinedOperation<K, V, T> {

	private final PipelinedOperation<K, V, T> delegate;
	private final WaitForReplication options;

	public WaitForReplicationOperation(PipelinedOperation<K, V, T> delegate, WaitForReplication options) {
		this.delegate = delegate;
		this.options = options;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Collection<RedisFuture> execute(StatefulConnection<K, V> connection, List<? extends T> items) {
		Collection<RedisFuture> futures = new ArrayList<>();
		futures.addAll(delegate.execute(connection, items));
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		PipelinedRedisFuture<Void> replicationFuture = new PipelinedRedisFuture<>(
				commands.waitForReplication(options.getReplicas(), options.getTimeout().toMillis()).thenAccept(r -> {
					if (r < options.getReplicas()) {
						throw new RedisCommandExecutionException(String.format(
								"Insufficient replication level - expected: %s, actual: %s", options.getReplicas(), r));
					}
				}));
		futures.add(replicationFuture);
		return futures;
	}

}
