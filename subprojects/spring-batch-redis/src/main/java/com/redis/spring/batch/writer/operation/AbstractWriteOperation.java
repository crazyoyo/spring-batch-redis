package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractWriteOperation<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> keyFunction;

	protected AbstractWriteOperation(Function<T, K> key) {
		Assert.notNull(key, "A key function is required");
		this.keyFunction = key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
		futures.add(execute(commands, item, keyFunction.apply(item)));
	}

	@SuppressWarnings("rawtypes")
	protected abstract RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key);

}
