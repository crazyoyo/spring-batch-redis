package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractWriteOperation<K, V, I, O> implements Operation<K, V, I, O> {

	private final Function<I, K> keyFunction;

	protected AbstractWriteOperation(Function<I, K> key) {
		Assert.notNull(key, "A key function is required");
		this.keyFunction = key;
	}

	@Override
	public RedisFuture<O> execute(BaseRedisAsyncCommands<K, V> commands, I item) {
		return execute(commands, item, keyFunction.apply(item));
	}

	protected abstract RedisFuture<O> execute(BaseRedisAsyncCommands<K, V> commands, I item, K key);

}
