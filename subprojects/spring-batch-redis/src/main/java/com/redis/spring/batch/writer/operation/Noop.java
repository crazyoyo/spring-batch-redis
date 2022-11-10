package com.redis.spring.batch.writer.operation;

import com.redis.spring.batch.common.NoOpRedisFuture;
import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements Operation<K, V, T> {

	@Override
	public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
	}

}
