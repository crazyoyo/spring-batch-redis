package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.common.BatchAsyncOperation;
import com.redis.spring.batch.common.DelegatingItemStreamSupport;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWaitOperation<K, V, T, U> extends DelegatingItemStreamSupport
		implements BatchAsyncOperation<K, V, T, U> {

	private final BatchAsyncOperation<K, V, T, Object> delegate;
	private final ReplicaWaitOptions options;

	public ReplicaWaitOperation(BatchAsyncOperation<K, V, T, Object> delegate, ReplicaWaitOptions options) {
		super(delegate);
		this.delegate = delegate;
		this.options = options;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<RedisFuture<U>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		futures.addAll(delegate.execute(commands, items));
		futures.add(replicationFuture(commands));
		return (List) futures;
	}

	private RedisFuture<?> replicationFuture(BaseRedisAsyncCommands<K, V> commands) {
		return new PipelinedRedisFuture<>(
				commands.waitForReplication(options.getReplicas(), options.getTimeout().toMillis()).thenAccept(r -> {
					if (r < options.getReplicas()) {
						throw new RedisCommandExecutionException(String.format(
								"Insufficient replication level - expected: %s, actual: %s", options.getReplicas(), r));
					}
				}));
	}

}
