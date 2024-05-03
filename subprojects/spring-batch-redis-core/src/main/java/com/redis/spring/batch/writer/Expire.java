package com.redis.spring.batch.writer;

import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private ToLongFunction<T> ttlFunction = t -> 0;

	public Expire(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	public Expire<K, V, T> ttl(long millis) {
		return ttl(t -> millis);
	}

	public Expire<K, V, T> ttl(ToLongFunction<T> function) {
		this.ttlFunction = function;
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> outputs) {
		long ttl = ttlFunction.applyAsLong(item);
		if (ttl > 0) {
			outputs.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, ttl));
		}
	}

}
