package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.common.BatchOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecBatchOperation<K, V, I> implements BatchOperation<K, V, I, Object> {

	private final BatchOperation<K, V, I, Object> delegate;

	public MultiExecBatchOperation(BatchOperation<K, V, I, Object> delegate) {
		this.delegate = delegate;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items) {
		List<RedisFuture<Object>> futures = new ArrayList<>(items.size());
		RedisTransactionalAsyncCommands<K, V> transactionalCommands = (RedisTransactionalAsyncCommands<K, V>) commands;
		futures.add((RedisFuture) transactionalCommands.multi());
		futures.addAll(delegate.execute(commands, items));
		futures.add((RedisFuture) transactionalCommands.exec());
		return futures;
	}

}
