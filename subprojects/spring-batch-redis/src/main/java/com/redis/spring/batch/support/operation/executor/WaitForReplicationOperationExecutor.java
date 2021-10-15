package com.redis.spring.batch.support.operation.executor;

import java.util.ArrayList;
import java.util.List;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.RedisFuture;

public class WaitForReplicationOperationExecutor<K, V, T> implements OperationExecutor<K, V, T> {

	private final OperationExecutor<K, V, T> delegate;
	private final int replicas;
	private final long timeout;

	public WaitForReplicationOperationExecutor(OperationExecutor<K, V, T> delegate, int replicas, long timeout) {
		this.delegate = delegate;
		this.replicas = replicas;
		this.timeout = timeout;
	}

	@Override
	public List<RedisFuture<?>> execute(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		futures.addAll(delegate.execute(commands, items));
		futures.add(commands.waitForReplication(replicas, this.timeout));
		return futures;
	}

}
