package com.redis.spring.batch.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class Rpush<K, V, T> extends AbstractKeyValueOperation<K, V, V, T> {

	public Rpush(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, V value) {
		return ((RedisListAsyncCommands<K, V>) commands).rpush(key, value);
	}

}
