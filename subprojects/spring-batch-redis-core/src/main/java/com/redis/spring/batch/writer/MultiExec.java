package com.redis.spring.batch.writer;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExec<K, V, T> implements WriteOperation<K, V, T> {

	private final WriteOperation<K, V, T> delegate;

	public MultiExec(WriteOperation<K, V, T> delegate) {
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
