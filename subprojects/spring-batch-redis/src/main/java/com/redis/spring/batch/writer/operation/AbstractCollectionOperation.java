package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractCollectionOperation<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Predicate<T> remove;

	protected AbstractCollectionOperation(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove) {
		super(key, delete);
		this.remove = remove;
	}

	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		if (remove.test(item)) {
			return remove(commands, item, key);
		}
		return add(commands, item, key);
	}

	protected abstract RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key);

	protected abstract RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key);

}
