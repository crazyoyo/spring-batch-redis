package com.redis.spring.batch.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class SaddAll<K, V, T> extends AbstractKeyValueOperation<K, V, Collection<V>, T> {

	public SaddAll(Function<T, K> keyFunction, Function<T, Collection<V>> valuesFunction) {
		super(keyFunction, valuesFunction, CollectionUtils::isEmpty);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Collection<V> value) {
		return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, (V[]) value.toArray());
	}

}
