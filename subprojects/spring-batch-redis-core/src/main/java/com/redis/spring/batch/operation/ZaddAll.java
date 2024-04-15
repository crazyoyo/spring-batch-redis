package com.redis.spring.batch.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractKeyValueOperation<K, V, Collection<ScoredValue<V>>, T> {

	private Function<T, ZAddArgs> argsFunction = t -> null;

	public ZaddAll(Function<T, K> keyFunction, Function<T, Collection<ScoredValue<V>>> valuesFunction) {
		super(keyFunction, valuesFunction, CollectionUtils::isEmpty);
	}

	public void setArgs(ZAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, ZAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key,
			Collection<ScoredValue<V>> value) {
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, argsFunction.apply(item),
				value.toArray(new ScoredValue[0]));
	}

}
