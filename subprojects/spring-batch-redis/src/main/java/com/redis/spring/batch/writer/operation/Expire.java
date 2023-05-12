package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, Long> milliseconds;

	public Expire(Function<T, K> key, Function<T, Long> millis) {
		super(key);
		Assert.notNull(millis, "A milliseconds function is required");
		this.milliseconds = millis;
	}

	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		Long millis = milliseconds.apply(item);
		if (millis != null && millis > 0) {
			execute(commands, key, millis);
		}
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Boolean> execute(BaseRedisAsyncCommands<K, V> commands, K key, long millis) {
		return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, millis);
	}

}
