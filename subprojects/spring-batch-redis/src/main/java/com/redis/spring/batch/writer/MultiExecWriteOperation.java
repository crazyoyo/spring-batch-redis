package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.spring.batch.common.BatchOperation;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecWriteOperation<K, V, T> implements BatchOperation<K, V, T, Object> {

	private final BatchOperation<K, V, T, ?> delegate;

	public MultiExecWriteOperation(BatchOperation<K, V, T, ?> delegate) {
		this.delegate = delegate;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<Future<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<Future> futures = new ArrayList<>();
		RedisTransactionalAsyncCommands<K, V> txCommands = (RedisTransactionalAsyncCommands<K, V>) commands;
		futures.add(txCommands.multi());
		futures.addAll(delegate.execute(commands, items));
		futures.add(txCommands.exec());
		return (List) futures;
	}

}
