package com.redis.spring.batch.item.redis.writer.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Del<K, V, T> implements Operation<K, V, T, Object> {

	private final Function<T, K> keyFunction;

	public Del(Function<T, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
		if (items.isEmpty()) {
			return Collections.emptyList();
		}
		return (List) Arrays.asList(commands.del((K[]) items.stream().map(keyFunction).toArray()));
	}

}
