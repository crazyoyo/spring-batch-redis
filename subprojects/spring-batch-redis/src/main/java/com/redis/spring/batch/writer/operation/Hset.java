package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.NoOpFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractWriteOperation<K, V, T, Long> {

	private final Function<T, Map<K, V>> map;

	public Hset(Function<T, K> key, Function<T, Map<K, V>> map) {
		super(key);
		Assert.notNull(map, "A map function is required");
		this.map = map;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Map<K, V> value = map.apply(item);
		if (value == null || value.isEmpty()) {
			return NoOpFuture.instance();
		}
		return ((RedisHashAsyncCommands<K, V>) commands).hset(key, value);
	}

}
