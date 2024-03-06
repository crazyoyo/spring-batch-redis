package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class RpushAll<K, V, T> extends AbstractPushAll<K, V, T> {

	public RpushAll(Function<T, K> keyFunction, Function<T, Collection<V>> valuesFunction) {
		super(keyFunction, valuesFunction);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected RedisFuture doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values) {
		return commands.rpush(key, values);
	}

}
