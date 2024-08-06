package com.redis.spring.batch.item.redis.writer.impl;

import java.util.ArrayList;
import java.util.List;

import com.redis.spring.batch.item.redis.common.AbstractCompositeOperation;
import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class MultiExec<K, V, T> extends AbstractCompositeOperation<K, V, T, Object> {

	public MultiExec(Operation<K, V, T, Object> delegate) {
		super(delegate);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
		List<RedisFuture<Object>> futures = new ArrayList<>();
		futures.add((RedisFuture) commands.multi());
		futures.addAll(delegate.execute(commands, items));
		futures.add((RedisFuture) commands.exec());
		return futures;
	}

}
