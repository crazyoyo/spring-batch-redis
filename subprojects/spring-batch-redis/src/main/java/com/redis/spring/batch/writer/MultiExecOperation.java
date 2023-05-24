package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.spring.batch.common.BatchAsyncOperation;
import com.redis.spring.batch.common.DelegatingItemStreamSupport;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class MultiExecOperation<K, V, T> extends DelegatingItemStreamSupport
		implements BatchAsyncOperation<K, V, T, Object> {

	private final BatchAsyncOperation<K, V, T, Object> delegate;

	public MultiExecOperation(BatchAsyncOperation<K, V, T, Object> delegate) {
		super(delegate);
		this.delegate = delegate;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<Future> futures = new ArrayList<>();
		RedisTransactionalAsyncCommands<K, V> txCommands = (RedisTransactionalAsyncCommands<K, V>) commands;
		futures.add(txCommands.multi());
		futures.addAll(delegate.execute(commands, items));
		futures.add(txCommands.exec());
		return (List) futures;
	}

}
