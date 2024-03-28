package com.redis.spring.batch.operation;

import java.time.Duration;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private Function<T, Duration> ttlFunction = t -> null;

	public Expire(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	public void setTtl(Duration duration) {
		this.ttlFunction = t -> duration;
	}

	public void setTtlFunction(Function<T, Duration> function) {
		this.ttlFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		Duration duration = ttlFunction.apply(item);
		if (duration != null && !duration.isNegative() && !duration.isZero()) {
			outputs.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, duration));
		}
	}

}
