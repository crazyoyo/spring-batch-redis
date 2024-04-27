package com.redis.spring.batch.writer;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Zadd<K, V, T> extends AbstractKeyValueOperation<K, V, ScoredValue<V>, T> {

	private Function<T, ZAddArgs> argsFunction = t -> null;

	public Zadd(Function<T, K> keyFunction, Function<T, ScoredValue<V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setArgs(ZAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, ZAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, ScoredValue<V> value) {
		ZAddArgs args = argsFunction.apply(item);
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, value);
	}

}
