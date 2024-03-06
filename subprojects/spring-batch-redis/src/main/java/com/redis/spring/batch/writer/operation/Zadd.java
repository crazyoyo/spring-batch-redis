package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Zadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, ScoredValue<V>> valueFunction;
	private Function<T, ZAddArgs> argsFunction = t -> null;

	public Zadd(Function<T, K> keyFunction, Function<T, ScoredValue<V>> valueFunction) {
		super(keyFunction);
		this.valueFunction = valueFunction;
	}

	public void setArgs(ZAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, ZAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		ZAddArgs args = argsFunction.apply(item);
		ScoredValue<V> value = valueFunction.apply(item);
		outputs.add((RedisFuture) ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, value));
	}

}
