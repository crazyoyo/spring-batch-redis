package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class SaddAll<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Collection<V>> valuesFunction;

	protected SaddAll(Function<T, K> keyFunction, Function<T, Collection<V>> valuesFunction) {
		super(keyFunction);
		this.valuesFunction = valuesFunction;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		Collection<V> collection = valuesFunction.apply(item);
		if (!CollectionUtils.isEmpty(collection)) {
			outputs.add((RedisFuture) ((RedisSetAsyncCommands<K, V>) commands).sadd(key, (V[]) collection.toArray()));
		}
	}

}
