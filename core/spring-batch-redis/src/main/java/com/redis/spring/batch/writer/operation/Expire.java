package com.redis.spring.batch.writer.operation;

import java.time.Duration;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private Function<T, Duration> ttlFunction = t -> null;

	public void setTtl(Duration duration) {
		this.ttlFunction = t -> duration;
	}

	public void setTtlFunction(Function<T, Duration> function) {
		this.ttlFunction = function;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Boolean> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Duration duration = ttlFunction.apply(item);
		if (duration == null || duration.isNegative() || duration.isZero()) {
			return null;
		}
		return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, duration);

	}

}
