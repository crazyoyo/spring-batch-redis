package com.redis.spring.batch.writer.operation;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.common.Operation;

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
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends T> inputs,
			Chunk<RedisFuture<Object>> outputs) {
		RedisTransactionalAsyncCommands<K, V> transactionalCommands = (RedisTransactionalAsyncCommands<K, V>) commands;
		outputs.add((RedisFuture) transactionalCommands.multi());
		delegate.execute(commands, inputs, outputs);
		outputs.add((RedisFuture) transactionalCommands.exec());
	}

}
