package com.redis.spring.batch.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class PushAll<K, V, T> extends AbstractKeyValueOperation<K, V, Collection<V>, T> {

	protected PushAll(Function<T, K> keyFunction, Function<T, Collection<V>> valuesFunction) {
		super(keyFunction, valuesFunction, CollectionUtils::isEmpty);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Collection<V> value) {
		V[] array = (V[]) value.toArray();
		return doPush((RedisListAsyncCommands<K, V>) commands, key, array);
	}

	protected abstract RedisFuture<Object> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
