package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.NoOpFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractWriteOperation<K, V, T> {

	private final Function<T, XAddArgs> args;
	private final Function<T, Map<K, V>> body;

	public Xadd(Function<T, K> key, Function<T, Map<K, V>> body, Function<T, XAddArgs> args) {
		super(key);
		Assert.notNull(body, "A body function is required");
		Assert.notNull(args, "Args function is required");
		this.body = body;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Map<K, V> map = body.apply(item);
		if (map.isEmpty()) {
			return NoOpFuture.instance();
		}
		return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args.apply(item), map);
	}

}
