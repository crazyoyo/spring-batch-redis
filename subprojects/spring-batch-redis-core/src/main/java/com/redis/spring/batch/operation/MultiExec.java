package com.redis.spring.batch.operation;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExec<K, V, T> implements Operation<K, V, T, Object> {

	private final Operation<K, V, T, Object> delegate;

	public MultiExec(Operation<K, V, T, Object> delegate) {
		this.delegate = delegate;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> inputs,
			List<RedisFuture<Object>> outputs) {
		RedisTransactionalAsyncCommands<K, V> transactionalCommands = (RedisTransactionalAsyncCommands<K, V>) commands;
		outputs.add((RedisFuture) transactionalCommands.multi());
		delegate.execute(commands, inputs, outputs);
		outputs.add((RedisFuture) transactionalCommands.exec());
	}

}
