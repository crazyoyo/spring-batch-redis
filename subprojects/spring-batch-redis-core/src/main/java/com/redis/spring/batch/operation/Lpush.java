package com.redis.spring.batch.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class Lpush<K, V, T> extends AbstractKeyValueOperation<K, V, V, T> {

	public Lpush(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, V value) {
		return ((RedisListAsyncCommands<K, V>) commands).lpush(key, value);
	}

}
