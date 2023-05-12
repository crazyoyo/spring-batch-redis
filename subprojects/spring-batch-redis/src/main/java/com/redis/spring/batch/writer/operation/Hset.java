package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, Map<K, V>> map;

	public Hset(Function<T, K> key, Function<T, Map<K, V>> map) {
		super(key);
		Assert.notNull(map, "A map function is required");
		this.map = map;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		Map<K, V> value = map.apply(item);
		if (value == null || value.isEmpty()) {
			return;
		}
		futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(key, value));
	}

}
