package com.redis.spring.batch.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractKeyValueOperation<K, V, Map<K, V>, T> {

	public Hset(Function<T, K> keyFunction, Function<T, Map<K, V>> mapFunction) {
		super(keyFunction, mapFunction, CollectionUtils::isEmpty);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Map<K, V> value) {
		return ((RedisHashAsyncCommands<K, V>) commands).hset(key, value);
	}

}
