package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V, T> extends AbstractWriteOperation<K, V, T, Object> {

	public static final Long TTL_KEY_DOES_NOT_EXIST = -2l;

	private final Function<T, byte[]> bytes;
	private final Function<T, Long> absoluteTtl;

	public Restore(Function<T, K> key, Function<T, byte[]> value, Function<T, Long> absoluteTTL) {
		super(key);
		Assert.notNull(value, "A value function is required");
		Assert.notNull(absoluteTTL, "A TTL function is required");
		this.bytes = value;
		this.absoluteTtl = absoluteTTL;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture<Object> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		byte[] dump = bytes.apply(item);
		Long ttl = absoluteTtl.apply(item);
		if (dump == null || TTL_KEY_DOES_NOT_EXIST.equals(ttl)) {
			return (RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(key);
		}
		return (RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).restore(key, dump, args(ttl));
	}

	protected RestoreArgs args(Long ttl) {
		if (ttl != null && ttl > 0) {
			return RestoreArgs.Builder.ttl(ttl).absttl();
		}
		return new RestoreArgs();
	}

}
