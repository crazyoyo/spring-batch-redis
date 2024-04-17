package com.redis.spring.batch.operation;

import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V, T> extends AbstractKeyValueOperation<K, V, byte[], T> {

	private ToLongFunction<T> ttlFunction = t -> 0;

	public Restore(Function<T, K> keyFunction, Function<T, byte[]> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setTtlFunction(ToLongFunction<T> function) {
		this.ttlFunction = function;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, byte[] value) {
		long ttl = ttlFunction.applyAsLong(item);
		if (value == null || ttl == KeyValue.TTL_NO_KEY) {
			return ((RedisKeyAsyncCommands<K, V>) commands).del(key);
		}
		RestoreArgs args = new RestoreArgs().replace(true);
		if (ttl > 0) {
			args.absttl().ttl(ttl);
		}
		return ((RedisKeyAsyncCommands<K, V>) commands).restore(key, value, args);
	}

}
