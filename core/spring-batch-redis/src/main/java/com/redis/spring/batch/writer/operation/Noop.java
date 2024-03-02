package com.redis.spring.batch.writer.operation;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Noop<K, V, T> implements Operation<K, V, T, Object> {

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends T> inputs,
			Chunk<RedisFuture<Object>> outputs) {
		// do nothing
	}

}
