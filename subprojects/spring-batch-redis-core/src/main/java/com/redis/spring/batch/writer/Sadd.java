package com.redis.spring.batch.writer;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractKeyValueOperation<K, V, V, T> {

	public Sadd(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction, valueFunction);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, V value) {
		return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, value);
	}

}
