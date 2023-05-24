package com.redis.spring.batch.writer.operation;

import com.redis.spring.batch.common.AsyncOperation;
import com.redis.spring.batch.common.NoOpFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, I, O> implements AsyncOperation<K, V, I, O> {

	@Override
	public RedisFuture<O> execute(BaseRedisAsyncCommands<K, V> commands, I item) {
		return NoOpFuture.instance();
	}

}
