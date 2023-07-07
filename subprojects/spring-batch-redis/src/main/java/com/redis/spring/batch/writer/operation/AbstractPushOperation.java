package com.redis.spring.batch.writer.operation;

import java.util.concurrent.Future;
import java.util.function.Function;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushOperation<K, V, T> extends AbstractWriteOperation<K, V, T> {

	private final Function<T, V> value;

	protected AbstractPushOperation(Function<T, K> key, Function<T, V> value) {
		super(key);
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Future<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return doPush((RedisListAsyncCommands<K, V>) commands, key, value.apply(item));
	}

	protected abstract Future<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V value);

}
