package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.NoOpFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> extends AbstractWriteOperation<K, V, T> {

	private final Function<T, Long> milliseconds;

	public Expire(Function<T, K> key, Function<T, Long> millis) {
		super(key);
		Assert.notNull(millis, "A milliseconds function is required");
		this.milliseconds = millis;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Boolean> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Long millis = milliseconds.apply(item);
		if (millis != null && millis > 0) {
			return execute((RedisKeyAsyncCommands<K, V>) commands, key, millis);
		}
		return NoOpFuture.instance();
	}

	protected RedisFuture<Boolean> execute(RedisKeyAsyncCommands<K, V> commands, K key, long millis) {
		return commands.pexpire(key, millis);
	}

}
