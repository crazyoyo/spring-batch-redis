package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class Lpush<K, V, T> extends AbstractPushOperation<K, V, T> {

	public Lpush(Function<T, K> key, Function<T, V> member) {
		super(key, member);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V value) {
		return commands.lpush(key, value);
	}

}
