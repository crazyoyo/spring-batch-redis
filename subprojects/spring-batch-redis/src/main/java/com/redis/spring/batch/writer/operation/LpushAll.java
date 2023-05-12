package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class LpushAll<K, V, T> extends AbstractPushAllOperation<K, V, T> {

	public LpushAll(Function<T, K> key, Function<T, Collection<V>> values) {
		super(key, values);
	}

	@Override
	protected RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values) {
		return commands.lpush(key, values);
	}

}
