package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAllOperation<K, V, T> extends AbstractAddAllOperation<K, V, T, V> {

	protected AbstractPushAllOperation(Function<T, K> key, Function<T, Collection<V>> values) {
		super(key, values);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Collection<V> values) {
		return doPush((RedisListAsyncCommands<K, V>) commands, key, (V[]) values.toArray());
	}

	protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
