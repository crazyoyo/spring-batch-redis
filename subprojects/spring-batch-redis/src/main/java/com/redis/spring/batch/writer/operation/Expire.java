package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Expire<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> keyFunction;
	private final Function<T, Long> millisFunction;

	public Expire(Function<T, K> key, Function<T, Long> millis) {
		this.keyFunction = key;
		Assert.notNull(millis, "A millisFunction function is required");
		this.millisFunction = millis;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
		K key = keyFunction.apply(item);
		Long millis = millisFunction.apply(item);
		if (millis != null && millis > 0) {
			futures.add((RedisFuture) execute((RedisKeyAsyncCommands<K, V>) commands, key, millis));
		}
	}

	protected RedisFuture<Boolean> execute(RedisKeyAsyncCommands<K, V> commands, K key, long millis) {
		return commands.pexpire(key, millis);
	}

}
