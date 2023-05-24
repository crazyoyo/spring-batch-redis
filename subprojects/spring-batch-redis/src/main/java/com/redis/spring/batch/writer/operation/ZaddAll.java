package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractAddAllOperation<K, V, T, ScoredValue<V>> {

	private final ZAddArgs args;

	public ZaddAll(Function<T, K> key, Function<T, Collection<ScoredValue<V>>> members) {
		this(key, members, null);
	}

	public ZaddAll(Function<T, K> key, Function<T, Collection<ScoredValue<V>>> members, ZAddArgs args) {
		super(key, members);
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key,
			Collection<ScoredValue<V>> values) {
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, values.toArray(new ScoredValue[0]));
	}

}
