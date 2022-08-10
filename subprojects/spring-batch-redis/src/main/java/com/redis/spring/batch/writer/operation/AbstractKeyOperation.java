package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyOperation<K, V, T> implements Operation<K, V, T> {

	private final Converter<T, K> keyConverter;
	private final Predicate<T> delPredicate;
	private final Operation<K, V, T> del;

	protected AbstractKeyOperation(Converter<T, K> key, Predicate<T> delPredicate) {
		this(key, delPredicate, new Del<>(key));
	}

	protected AbstractKeyOperation(Converter<T, K> key, Predicate<T> delPredicate, Operation<K, V, T> del) {
		Assert.notNull(key, "A key converter is required");
		Assert.notNull(delPredicate, "A delete predicate is required");
		this.keyConverter = key;
		this.delPredicate = delPredicate;
		this.del = del;
	}

	@Override
	public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		if (delPredicate.test(item)) {
			return del.execute(commands, item);
		}
		return doExecute(commands, item, this.keyConverter.convert(item));
	}

	protected abstract RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key);

}
