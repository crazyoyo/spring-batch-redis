package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractAddAllOperation<K, V, T, U> extends AbstractOperation<K, V, T> {

	private final Function<T, Collection<U>> values;

	protected AbstractAddAllOperation(Function<T, K> key, Function<T, Collection<U>> values) {
		super(key);
		this.values = values;
	}

	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		execute(commands, futures, item, key, values.apply(item));
	}

	protected abstract void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key,
			Collection<U> values);

}
