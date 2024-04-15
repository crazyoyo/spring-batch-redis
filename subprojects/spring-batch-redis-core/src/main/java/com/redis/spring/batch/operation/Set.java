package com.redis.spring.batch.operation;

import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class Set<K, V, T> extends AbstractKeyValueOperation<K, V, V, T> {

	private static final SetArgs DEFAULT_ARGS = new SetArgs();

	private Function<T, SetArgs> argsFunction = t -> DEFAULT_ARGS;

	public Set(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setArgs(SetArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, SetArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, V value) {
		SetArgs args = argsFunction.apply(item);
		return ((RedisStringAsyncCommands<K, V>) commands).set(key, value, args);
	}

}
