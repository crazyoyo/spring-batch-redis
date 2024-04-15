package com.redis.spring.batch.operation;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class CompositeOperation<K, V, I, O> implements Operation<K, V, I, O> {

	private final Collection<Operation<K, V, I, O>> delegates;

	@SuppressWarnings("unchecked")
	public CompositeOperation(Operation<K, V, I, O>... delegates) {
		this(Arrays.asList(delegates));
	}

	public CompositeOperation(Collection<Operation<K, V, I, O>> delegates) {
		this.delegates = delegates;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends I> inputs,
			List<RedisFuture<O>> outputs) {
		delegates.forEach(o -> o.execute(commands, inputs, outputs));
	}

}
