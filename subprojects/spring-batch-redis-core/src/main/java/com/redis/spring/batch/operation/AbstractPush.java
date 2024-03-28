package com.redis.spring.batch.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPush<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	protected AbstractPush(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	private Function<T, V> valueFunction;

	public void setValueFunction(Function<T, V> function) {
		this.valueFunction = function;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		V value = valueFunction.apply(item);
		outputs.add((RedisFuture) doPush((RedisListAsyncCommands<K, V>) commands, key, value));
	}

	protected abstract RedisFuture<Object> doPush(RedisListAsyncCommands<K, V> commands, K key, V value);

}
