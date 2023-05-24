package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class SaddAll<K, V, T> extends AbstractAddAllOperation<K, V, T, V> {

	public SaddAll(Function<T, K> key, Function<T, Collection<V>> values) {
		super(key, values);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Collection<V> values) {
		return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, (V[]) values.toArray());
	}

}
