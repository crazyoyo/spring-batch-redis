package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class Set<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private static final SetArgs DEFAULT_ARGS = new SetArgs();

	private final Function<T, V> valueFunction;

	private Function<T, SetArgs> argsFunction = t -> DEFAULT_ARGS;

	public Set(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction);
		this.valueFunction = valueFunction;
	}

	public void setArgs(SetArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, SetArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		V value = valueFunction.apply(item);
		SetArgs args = argsFunction.apply(item);
		outputs.add((RedisFuture) ((RedisStringAsyncCommands<K, V>) commands).set(key, value, args));
	}

}
