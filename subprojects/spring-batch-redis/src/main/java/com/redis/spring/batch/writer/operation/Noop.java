package com.redis.spring.batch.writer.operation;

import com.redis.spring.batch.writer.RedisOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements RedisOperation<K, V, T> {

	@Override
	public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		return null;
	}

}
