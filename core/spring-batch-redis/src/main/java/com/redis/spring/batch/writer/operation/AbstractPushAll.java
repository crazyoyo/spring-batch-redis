package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public abstract class AbstractPushAll<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Collection<V>> valuesFunction;

	protected AbstractPushAll(Function<T, K> keyFunction, Function<T, Collection<V>> valuesFunction) {
		super(keyFunction);
		this.valuesFunction = valuesFunction;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		Collection<V> values = valuesFunction.apply(item);
		if (!CollectionUtils.isEmpty(values)) {
			RedisListAsyncCommands<K, V> listCommands = (RedisListAsyncCommands<K, V>) commands;
			V[] array = (V[]) values.toArray();
			outputs.add(doPush(listCommands, key, array));
		}
	}

	protected abstract RedisFuture<Object> doPush(RedisListAsyncCommands<K, V> commands, K key, V[] values);

}
