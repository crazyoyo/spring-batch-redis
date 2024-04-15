package com.redis.spring.batch.operation;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.PipelinedRedisFuture;

public class ReplicaWait<K, V, T> implements Operation<K, V, T, Object> {

	private final Operation<K, V, T, Object> delegate;

	private final int replicas;

	private final long timeout;

	public ReplicaWait(Operation<K, V, T, Object> delegate, int replicas, Duration timeout) {
		this.delegate = delegate;
		this.replicas = replicas;
		this.timeout = timeout.toMillis();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> items,
			List<RedisFuture<Object>> outputs) {
		delegate.execute(commands, items, outputs);
		RedisFuture<Long> waitFuture = commands.waitForReplication(replicas, timeout);
		outputs.add((RedisFuture) new PipelinedRedisFuture<>(waitFuture.thenAccept(this::checkReplicas)));
	}

	private void checkReplicas(Long actual) {
		if (actual == null || actual < replicas) {
			throw new RedisCommandExecutionException(errorMessage(actual));
		}
	}

	private String errorMessage(Long actual) {
		return MessageFormat.format("Insufficient replication level ({0}/{1})", actual, replicas);
	}

}
