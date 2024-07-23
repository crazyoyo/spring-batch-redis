package com.redis.spring.batch.item.redis.writer.operation;

import java.util.Collections;
import java.util.List;

import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Noop<K, V, T> implements Operation<K, V, T, Object> {

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
		return Collections.emptyList();
	}

}
