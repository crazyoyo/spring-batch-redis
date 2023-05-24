package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Zadd<K, V, T> extends AbstractWriteOperation<K, V, T, Long> {

	private final Function<T, ScoredValue<V>> value;
	private final Function<T, ZAddArgs> args;

	public Zadd(Function<T, K> key, Function<T, ScoredValue<V>> value) {
		this(key, value, t -> null);
	}

	public Zadd(Function<T, K> key, Function<T, ScoredValue<V>> value, Function<T, ZAddArgs> args) {
		super(key);
		this.value = value;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args.apply(item), value.apply(item));
	}

}
