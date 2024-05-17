package com.redis.spring.batch.item.redis.writer.operation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		List<K> keys = StreamSupport.stream(items.spliterator(), false).map(keyFunction).collect(Collectors.toList());
		if (keys.isEmpty()) {
			return Collections.emptyList();
		}
		return Arrays.asList((RedisFuture) commands.del((K[]) keys.toArray()));
	}

}
