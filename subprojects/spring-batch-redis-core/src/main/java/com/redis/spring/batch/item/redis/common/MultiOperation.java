package com.redis.spring.batch.item.redis.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class MultiOperation<K, V, I, O> implements InitializingOperation<K, V, I, O> {

	protected final Iterable<Operation<K, V, I, O>> delegates;

	@SuppressWarnings("unchecked")
	public MultiOperation(Operation<K, V, I, O>... delegates) {
		this(Arrays.asList(delegates));
	}

	public MultiOperation(Iterable<Operation<K, V, I, O>> delegates) {
		this.delegates = delegates;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		for (Operation<K, V, I, O> delegate : delegates) {
			if (delegate instanceof InitializingOperation) {
				((InitializingOperation<K, V, I, O>) delegate).afterPropertiesSet();
			}
		}
	}

	@Override
	public void setClient(AbstractRedisClient client) {
		for (Operation<K, V, I, O> delegate : delegates) {
			if (delegate instanceof InitializingOperation) {
				((InitializingOperation<K, V, I, O>) delegate).setClient(client);
			}
		}
	}

	@Override
	public List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items) {
		List<RedisFuture<O>> futures = new ArrayList<>();
		for (Operation<K, V, I, O> delegate : delegates) {
			futures.addAll(delegate.execute(commands, items));
		}
		return futures;
	}

}
