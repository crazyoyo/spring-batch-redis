package com.redis.spring.batch.writer.operation;

import java.util.List;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements WriteOperation<K, V, T> {

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		// do nothing
	}

}
