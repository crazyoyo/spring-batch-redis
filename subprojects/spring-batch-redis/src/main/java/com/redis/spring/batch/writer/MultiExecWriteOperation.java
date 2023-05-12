package com.redis.spring.batch.writer;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecWriteOperation<K, V, T> implements BatchWriteOperation<K, V, T> {

	private BatchWriteOperation<K, V, T> operation;

	public MultiExecWriteOperation(BatchWriteOperation<K, V, T> operation) {
		this.operation = operation;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, List<? extends T> items) {
		futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).multi());
		operation.execute(commands, futures, items);
		futures.add(((RedisTransactionalAsyncCommands<K, V>) commands).exec());
	}

}
