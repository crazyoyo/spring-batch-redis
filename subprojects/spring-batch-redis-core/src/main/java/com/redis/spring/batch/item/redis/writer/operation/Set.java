package com.redis.spring.batch.item.redis.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.writer.AbstractValueWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Set<K, V, T> extends AbstractValueWriteOperation<K, V, V, T> {

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

	private SetArgs args(T item) {
		return argsFunction.apply(item);
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	private RedisFuture<?> execute(RedisAsyncCommands<K, V> commands, T item) {
		return commands.set(key(item), value(item), args(item));
	}

}
