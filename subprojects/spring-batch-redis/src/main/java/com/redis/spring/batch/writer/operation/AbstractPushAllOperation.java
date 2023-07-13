package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAllOperation<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> key;
	private final Function<T, Collection<V>> valuesFunction;

	protected AbstractPushAllOperation(Function<T, K> key, Function<T, Collection<V>> values) {
		this.key = key;
		this.valuesFunction = values;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
		futures.add((RedisFuture) doPush((RedisListAsyncCommands<K, V>) commands, key.apply(item),
				(V[]) valuesFunction.apply(item).toArray()));
	}

	protected abstract RedisFuture<Long> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
