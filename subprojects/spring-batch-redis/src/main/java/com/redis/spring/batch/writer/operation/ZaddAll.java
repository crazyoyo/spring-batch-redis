package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Collection<ScoredValue<V>>> valuesFunction;
	private Function<T, ZAddArgs> argsFunction = t -> null;

	public ZaddAll(Function<T, K> keyFunction, Function<T, Collection<ScoredValue<V>>> valuesFunction) {
		super(keyFunction);
		this.valuesFunction = valuesFunction;
	}

	public void setArgs(ZAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, ZAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		Collection<ScoredValue<V>> values = valuesFunction.apply(item);
		if (!CollectionUtils.isEmpty(values)) {
			ZAddArgs args = argsFunction.apply(item);
			ScoredValue<V>[] array = values.toArray(new ScoredValue[0]);
			outputs.add((RedisFuture) ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, array));
		}
	}

}
