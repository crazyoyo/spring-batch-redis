package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractOperation<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> keyFunction;

	protected AbstractOperation(Function<T, K> key) {
		Assert.notNull(key, "A key function is required");
		this.keyFunction = key;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		execute(commands, futures, item, keyFunction.apply(item));
	}

	protected abstract void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key);

}
