package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import com.redis.spring.batch.common.NoOpFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractAddAllOperation<K, V, I, O> extends AbstractWriteOperation<K, V, I, Long> {

	private final Function<I, Collection<O>> values;

	protected AbstractAddAllOperation(Function<I, K> key, Function<I, Collection<O>> values) {
		super(key);
		this.values = values;
	}

	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> context, I item, K key) {
		Collection<O> collection = values.apply(item);
		if (collection.isEmpty()) {
			return NoOpFuture.instance();
		}
		return execute(context, item, key, collection);
	}

	protected abstract RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> context, I item, K key,
			Collection<O> values);

}
