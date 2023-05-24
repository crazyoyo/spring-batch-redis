package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.KeyTtlValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V, T> extends AbstractWriteOperation<K, V, T, Object> {

	private final Function<T, byte[]> bytes;
	private final Function<T, Long> ttlFunction;

	public Restore(Function<T, K> key, Function<T, byte[]> value, Function<T, Long> absoluteTTL) {
		super(key);
		Assert.notNull(value, "A value function is required");
		Assert.notNull(absoluteTTL, "A TTL function is required");
		this.bytes = value;
		this.ttlFunction = absoluteTTL;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture<Object> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		byte[] dump = bytes.apply(item);
		if (dump == null) {
			return (RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(key);
		}
		return (RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).restore(key, dump, args(item));
	}

	protected RestoreArgs args(T item) {
		Long ttl = ttlFunction.apply(item);
		if (KeyTtlValue.isPositiveTtl(ttl)) {
			return RestoreArgs.Builder.ttl(ttl).absttl();
		}
		return new RestoreArgs();
	}

}
