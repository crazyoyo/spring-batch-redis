package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DelegatingItemStreamSupport;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWaitWriteOperation<K, V, T, U> extends DelegatingItemStreamSupport
		implements BatchOperation<K, V, T, U> {

	private final BatchOperation<K, V, T, ?> delegate;
	private ReplicaWaitOptions options = ReplicaWaitOptions.builder().build();

	public ReplicaWaitWriteOperation(BatchOperation<K, V, T, ?> delegate) {
		super(delegate);
		this.delegate = delegate;
	}

	public ReplicaWaitOptions getOptions() {
		return options;
	}

	public void setOptions(ReplicaWaitOptions options) {
		this.options = options;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<Future<U>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<Future<?>> futures = new ArrayList<>();
		futures.addAll(delegate.execute(commands, items));
		futures.add(replicationFuture(commands));
		return (List) futures;
	}

	private RedisFuture<?> replicationFuture(BaseRedisAsyncCommands<K, V> commands) {
		return new PipelinedRedisFuture<>(
				commands.waitForReplication(options.getReplicas(), options.getTimeout().toMillis())
						.thenAccept(this::checkReplicas));
	}

	private void checkReplicas(Long actual) {
		if (actual == null || actual < options.getReplicas()) {
			throw new InsufficientReplicasException(options.getReplicas(), actual);
		}
	}

	private static class InsufficientReplicasException extends RedisCommandExecutionException {

		private static final long serialVersionUID = 1L;
		private static final String MESSAGE = "Insufficient replication level - expected: %s, actual: %s";

		public InsufficientReplicasException(long expected, long actual) {
			super(String.format(MESSAGE, expected, actual));
		}

	}

}
