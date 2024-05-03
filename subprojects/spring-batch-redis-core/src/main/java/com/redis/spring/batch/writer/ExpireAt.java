package com.redis.spring.batch.writer;

import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class ExpireAt<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private ToLongFunction<T> epochFunction = t -> 0;

	public ExpireAt(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	public ExpireAt<K, V, T> epoch(long epoch) {
		return epoch(t -> epoch);
	}

	public ExpireAt<K, V, T> epoch(ToLongFunction<T> function) {
		this.epochFunction = function;
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> outputs) {
		long ttl = epochFunction.applyAsLong(item);
		if (ttl > 0) {
			outputs.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(key, ttl));
		}
	}

	public static <K, V, T> ExpireAt<K, V, T> of(Function<T, K> key, ToLongFunction<T> epoch) {
		return new ExpireAt<K, V, T>(key).epoch(epoch);
	}

}
