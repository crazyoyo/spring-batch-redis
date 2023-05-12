package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, V> value;

	public Sadd(Function<T, K> key, Function<T, V> value) {
		super(key);
		this.value = value;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(key, value.apply(item)));
	}

}
