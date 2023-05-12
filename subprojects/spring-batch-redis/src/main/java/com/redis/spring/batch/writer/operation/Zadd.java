package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Zadd<K, V, T> extends AbstractOperation<K, V, T> {

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

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args.apply(item), value.apply(item)));
	}

}
