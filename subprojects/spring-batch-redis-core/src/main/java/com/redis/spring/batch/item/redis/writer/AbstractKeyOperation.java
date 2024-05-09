package com.redis.spring.batch.item.redis.writer;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyOperation<K, V, I> implements WriteOperation<K, V, I> {

	private final Function<I, K> keyFunction;

	protected AbstractKeyOperation(Function<I, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends I> inputs,
			List<RedisFuture<Object>> outputs) {
		for (I item : inputs) {
			K key = keyFunction.apply(item);
			execute(commands, item, key, outputs);
		}
	}

	protected abstract void execute(BaseRedisAsyncCommands<K, V> commands, I item, K key,
			List<RedisFuture<Object>> outputs);

}
