package com.redis.spring.batch.writer.operation;

import java.util.concurrent.Future;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractWriteOperation<K, V, I> implements Operation<K, V, I, Object> {

	private final Function<I, K> keyFunction;

	protected AbstractWriteOperation(Function<I, K> key) {
		Assert.notNull(key, "A key function is required");
		this.keyFunction = key;
	}

	@Override
	public Future execute(BaseRedisAsyncCommands<K, V> commands, I item) {
		return execute(commands, item, keyFunction.apply(item));
	}

	protected abstract Future execute(BaseRedisAsyncCommands<K, V> commands, I item, K key);

}
