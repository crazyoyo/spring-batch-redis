package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyWriteOperation<K, V, I> implements Operation<K, V, I, Object> {

	private final Function<I, K> keyFunction;

	protected AbstractKeyWriteOperation(Function<I, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends I> inputs,
			Chunk<RedisFuture<Object>> outputs) {
		for (I item : inputs) {
			K key = keyFunction.apply(item);
			execute(commands, item, key, outputs);
		}
	}

	protected abstract void execute(BaseRedisAsyncCommands<K, V> commands, I item, K key,
			Chunk<RedisFuture<Object>> outputs);

}
