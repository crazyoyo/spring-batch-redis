package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushOperation<K, V, T> extends AbstractOperation<K, V, T> {

	private final Function<T, V> value;

	protected AbstractPushOperation(Function<T, K> key, Function<T, V> value) {
		super(key);
		this.value = value;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(doPush((RedisListAsyncCommands<K, V>) commands, key, value.apply(item)));
	}

	protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V value);

}
