package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAllOperation<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> keyFunction;
	private final Function<T, Collection<V>> valuesFunction;

	protected AbstractPushAllOperation(Function<T, K> key, Function<T, Collection<V>> values) {
		this.keyFunction = key;
		this.valuesFunction = values;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
		Collection<V> values = valuesFunction.apply(item);
		if (values.isEmpty()) {
			return;
		}
		RedisListAsyncCommands<K, V> listCommands = (RedisListAsyncCommands<K, V>) commands;
		K key = keyFunction.apply(item);
		futures.add((RedisFuture) doPush(listCommands, key, (V[]) values.toArray()));
	}

	protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
