package com.redis.spring.batch.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class LpushAll<K, V, T> extends AbstractPushAll<K, V, T> {

	public LpushAll(Function<T, K> keyFunction, Function<T, Collection<V>> valuesFunction) {
		super(keyFunction, valuesFunction);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected RedisFuture doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values) {
		return commands.lpush(key, values);
	}

}
