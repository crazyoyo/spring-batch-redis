package com.redis.spring.batch.writer.operation;

import java.util.function.Function;
import java.util.function.ToLongFunction;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> extends Expire<K, V, T> {

	public ExpireAt(Function<T, K> key, ToLongFunction<T> millis) {
		super(key, millis);
	}

	@Override
	protected RedisFuture<Boolean> execute(RedisKeyAsyncCommands<K, V> commands, K key, long millis) {
		return commands.pexpireat(key, millis);
	}

}
