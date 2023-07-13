package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Del<K, V, T> extends AbstractWriteOperation<K, V, T> {

	public Del(Function<T, K> key) {
		super(key);
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisKeyAsyncCommands<K, V>) commands).del(key);
	}

}
