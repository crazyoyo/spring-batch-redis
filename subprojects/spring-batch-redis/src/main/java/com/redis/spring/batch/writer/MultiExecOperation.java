package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecOperation<K, V, T> implements PipelinedOperation<K, V, T> {

	private PipelinedOperation<K, V, T> operation;

	public MultiExecOperation(PipelinedOperation<K, V, T> operation) {
		this.operation = operation;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<RedisFuture<?>> execute(StatefulConnection<K, V> connection, List<? extends T> items) {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<?>> futures = new ArrayList<>();
		futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).multi());
		futures.addAll(operation.execute(connection, items));
		futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).exec());
		return futures;
	}

}
