package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.KeyTtlValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, byte[]> bytes;
	private final Function<T, Long> ttlFunction;

	public Restore(Function<T, K> key, Function<T, byte[]> value, Function<T, Long> absoluteTTL) {
		super(key);
		Assert.notNull(value, "A value function is required");
		Assert.notNull(absoluteTTL, "A TTL function is required");
		this.bytes = value;
		this.ttlFunction = absoluteTTL;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		Long ttl = this.ttlFunction.apply(item);
		if (KeyTtlValue.isInexistentKeyTtl(ttl)) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(key));
		} else {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(key, bytes.apply(item), args(ttl)));
		}
	}

	protected RestoreArgs args(Long ttl) {
		if (KeyTtlValue.isPositiveTtl(ttl)) {
			return RestoreArgs.Builder.ttl(ttl).absttl();
		}
		return new RestoreArgs();
	}

}
