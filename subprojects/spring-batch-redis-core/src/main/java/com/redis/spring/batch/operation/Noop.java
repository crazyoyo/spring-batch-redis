package com.redis.spring.batch.operation;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements Operation<K, V, T, Object> {

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends T> inputs,
			List<RedisFuture<Object>> outputs) {
		// do nothing
	}

}
